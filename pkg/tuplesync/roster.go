package tuplesync

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"google.golang.org/protobuf/types/known/structpb"
)

// Roster is the team-permission list: the single source of truth for the
// tuples managed by one source in one target store.
type Roster struct {
	// StoreID is the target OpenFGA store to reconcile.
	StoreID string `json:"store_id"`
	// Source identifies the owning roster (e.g. "team-directory"). The same
	// target store may host tuples from several sources; they must never
	// claim the same tuple key.
	Source string `json:"source"`
	// AuthorizationModelID optionally pins the model used to validate and
	// write the tuples. Empty means the store's latest model.
	AuthorizationModelID string `json:"authorization_model_id,omitempty"`
	// Tuples is the desired full set of managed tuples.
	Tuples []RosterEntry `json:"tuples"`
}

// RosterEntry is one desired tuple in a Roster.
type RosterEntry struct {
	Object    string           `json:"object"`
	Relation  string           `json:"relation"`
	User      string           `json:"user"`
	Condition *RosterCondition `json:"condition,omitempty"`
}

// RosterCondition is the optional ABAC condition attached to a roster tuple.
type RosterCondition struct {
	// Name must reference a condition defined in the target authorization
	// model whenever Condition is present.
	Name string `json:"name"`
	// Context is the raw JSON object supplied as condition context. It is
	// kept raw so ParseRoster can reject non-object shapes before touching
	// protobuf; it is converted to structpb.Struct in toTupleKey.
	Context json.RawMessage `json:"context,omitempty"`
}

// ParseRoster decodes a roster JSON document and runs all static validation
// (envelope fields, per-entry shape, duplicates). A JSON syntax/shape error
// is returned as the error; semantic problems are returned as Issues and are
// also attached to the returned roster via Validate, so callers may adjust
// fields (for example CLI flag overrides) and re-validate.
func ParseRoster(r io.Reader) (*Roster, []Issue, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, fmt.Errorf("read roster: %w", err)
	}

	var roster Roster
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&roster); err != nil {
		return nil, nil, fmt.Errorf("parse roster JSON: %w", err)
	}
	if dec.More() {
		return nil, nil, fmt.Errorf("parse roster JSON: unexpected trailing data")
	}

	issues := roster.Validate()
	return &roster, issues, nil
}

// Validate runs all static roster checks and returns deterministic, sorted
// issues. It does not perform model-level validation (that requires the
// target store and is done by the Planner).
func (r *Roster) Validate() []Issue {
	var issues []Issue

	if r.StoreID == "" || r.Source == "" || len(r.Tuples) == 0 || !r.sourceShapeValid() {
		problems := make([]string, 0, 4)
		if r.StoreID == "" {
			problems = append(problems, "store_id is required")
		}
		if r.Source == "" {
			problems = append(problems, "source is required")
		} else if !r.sourceShapeValid() {
			problems = append(problems, fmt.Sprintf(
				"source %q is invalid: use at most %d characters without ':', '#', '@' or spaces (it is embedded in control-store ledger users)",
				r.Source, maxSourceIDLength))
		}
		if len(r.Tuples) == 0 {
			problems = append(problems, "tuples must contain at least one entry")
		}
		issues = append(issues, Issue{
			Code:    IssueMalformedRoster,
			Message: fmt.Sprintf("roster is invalid: %v", problems),
		})
	}

	seen := make(map[string]int, len(r.Tuples))
	for i := range r.Tuples {
		e := r.Tuples[i]
		key := e.keyString()
		if msg := e.validate(); msg != "" {
			issues = append(issues, Issue{
				Code:     IssueMalformedTuple,
				TupleKey: key,
				Message:  msg,
			})
		}
		if first, ok := seen[key]; ok {
			issues = append(issues, Issue{
				Code:     IssueDuplicateInRoster,
				TupleKey: key,
				Message: fmt.Sprintf(
					"tuple %q is listed more than once (entries %d and %d); a key may appear exactly once even when conditions differ; remove the duplicate",
					key, first+1, i+1),
			})
		} else {
			seen[key] = i
		}
	}

	sortIssues(issues)
	return issues
}

func (e RosterEntry) validate() string {
	switch {
	case e.Object == "":
		return "tuple is missing object; expected form \"<type>:<id>\""
	case !tupleUtils.IsValidObject(e.Object):
		return fmt.Sprintf("object %q is malformed; expected exactly one ':' separating type and id, no '#' or spaces", e.Object)
	case e.Relation == "":
		return "tuple is missing relation"
	case !tupleUtils.IsValidRelation(e.Relation):
		return fmt.Sprintf("relation %q is malformed; must not contain ':', '#', '@' or spaces", e.Relation)
	case e.User == "":
		return "tuple is missing user; expected \"<type>:<id>\", \"<type>:<id>#<relation>\" or \"<type>:*\""
	case !tupleUtils.IsValidUser(e.User):
		return fmt.Sprintf("user %q is malformed; expected a typed object, userset or typed wildcard without spaces", e.User)
	}

	if c := e.Condition; c != nil {
		if c.Name == "" {
			return "condition.name is required when a condition is present; drop the condition block or reference a model-defined condition name"
		}
		raw := bytes.TrimSpace(c.Context)
		if len(raw) > 0 {
			if !json.Valid(raw) {
				return "condition.context is not valid JSON"
			}
			if !bytes.Equal(raw, []byte("null")) {
				var m map[string]json.RawMessage
				if err := json.Unmarshal(raw, &m); err != nil {
					return "condition.context must be a JSON object with parameter names as keys"
				}
			}
		}
	}
	return ""
}

// sourceShapeValid validates the source id exactly as it is embedded in
// "fga_source:<source>" ledger users: same charset as a tuple user id and
// short enough for the most restrictive backend column.
func (r *Roster) sourceShapeValid() bool {
	return r.Source != "" &&
		len(r.Source) <= maxSourceIDLength &&
		tupleUtils.IsValidUserID(r.Source)
}

// keyString renders the tuple identity (condition excluded) as
// "object#relation@user".
func (e RosterEntry) keyString() string {
	return tupleUtils.TupleKeyToString(&openfgav1.TupleKeyWithoutCondition{
		Object:   e.Object,
		Relation: e.Relation,
		User:     e.User,
	})
}

// toTupleKey converts the validated entry to an openfgav1.TupleKey.
func (e RosterEntry) toTupleKey() (*openfgav1.TupleKey, error) {
	tk := &openfgav1.TupleKey{Object: e.Object, Relation: e.Relation, User: e.User}
	if c := e.Condition; c != nil {
		tk.Condition = &openfgav1.RelationshipCondition{Name: c.Name}
		if raw := bytes.TrimSpace(c.Context); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, fmt.Errorf("condition context for %s: %w", e.keyString(), err)
			}
			s, err := structpb.NewStruct(m)
			if err != nil {
				return nil, fmt.Errorf("condition context for %s: %w", e.keyString(), err)
			}
			tk.Condition.Context = s
		}
	}
	return tk, nil
}

func sortIssues(issues []Issue) {
	sort.SliceStable(issues, func(i, j int) bool {
		if issues[i].Code != issues[j].Code {
			return issues[i].Code < issues[j].Code
		}
		if issues[i].TupleKey != issues[j].TupleKey {
			return issues[i].TupleKey < issues[j].TupleKey
		}
		return issues[i].Message < issues[j].Message
	})
}
