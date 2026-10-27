package tuplesync

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// conditionPayload is the canonical, JSON-serializable shape of an
// openfgav1.RelationshipCondition used inside tuplePayload.
type conditionPayload struct {
	Name    string          `json:"name"`
	Context json.RawMessage `json:"context,omitempty"`
}

// tuplePayload is the canonical JSON representation of a managed tuple,
// excluding the target store id. Field order is fixed and context is produced
// by encoding/json over a map, whose keys are sorted, so the byte slice is
// deterministic for semantically equal input.
type tuplePayload struct {
	Object    string            `json:"object"`
	Relation  string            `json:"relation"`
	User      string            `json:"user"`
	Condition *conditionPayload `json:"condition,omitempty"`
}

// CanonicalKey is the stable identity of a managed tuple scoped to one target
// store. It is the basis of the deterministic claim id and of ledger payload
// encoding.
type CanonicalKey struct {
	StoreID       string
	Object        string
	Relation      string
	User          string
	ConditionName string
	// contextJSON is the canonical (key-sorted) JSON encoding of the
	// condition context, or empty when the tuple carries no condition.
	contextJSON  string
	hasCondition bool
}

// newCanonicalKey normalizes an openfgav1.TupleKey for the given target store.
// Context protobufs round-trip through structpb.Struct.AsMap so that two
// contexts differing only in field order canonicalize identically.
func newCanonicalKey(storeID string, tk *openfgav1.TupleKey) (CanonicalKey, error) {
	k := CanonicalKey{
		StoreID:  storeID,
		Object:   tk.GetObject(),
		Relation: tk.GetRelation(),
		User:     tk.GetUser(),
	}

	if cond := tk.GetCondition(); cond != nil {
		k.hasCondition = true
		k.ConditionName = cond.GetName()
		if ctx := cond.GetContext(); ctx != nil {
			raw, err := canonicalContextJSON(ctx)
			if err != nil {
				return CanonicalKey{}, err
			}
			k.contextJSON = raw
		}
	}
	return k, nil
}

// HasCondition reports whether the tuple carries a relationship condition.
func (k CanonicalKey) HasCondition() bool { return k.hasCondition }

// ContextJSON returns the canonical condition context JSON ("" if none).
func (k CanonicalKey) ContextJSON() string { return k.contextJSON }

func canonicalContextJSON(ctx *structpb.Struct) (string, error) {
	raw, err := json.Marshal(ctx.AsMap())
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// marshalPayload renders the canonical payload JSON (without store id).
func (k CanonicalKey) marshalPayload() ([]byte, error) {
	p := tuplePayload{Object: k.Object, Relation: k.Relation, User: k.User}
	if k.hasCondition {
		p.Condition = &conditionPayload{Name: k.ConditionName}
		if k.contextJSON != "" {
			p.Condition.Context = json.RawMessage(k.contextJSON)
		}
	}
	return json.Marshal(p)
}

// Payload returns the canonical payload JSON string. It is used verbatim as
// the ledger claim payload (chunked) and in tests.
func (k CanonicalKey) Payload() string {
	raw, err := k.marshalPayload()
	if err != nil {
		// Every input is built from JSON-safe structpb values / strings, so
		// marshaling cannot fail in practice.
		panic(err)
	}
	return string(raw)
}

// Hash is the deterministic claim id: SHA-256 over the target store and the
// canonical payload, rendered as lowercase hex.
func (k CanonicalKey) Hash() string {
	raw := k.StoreID + "\n" + k.Payload()
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}

// TupleKey reconstructs the openfgav1.TupleKey represented by this canonical
// key. Condition context is re-parsed from its canonical JSON.
func (k CanonicalKey) TupleKey() (*openfgav1.TupleKey, error) {
	tk := &openfgav1.TupleKey{Object: k.Object, Relation: k.Relation, User: k.User}
	if k.hasCondition {
		tk.Condition = &openfgav1.RelationshipCondition{Name: k.ConditionName}
		if k.contextJSON != "" {
			var m map[string]any
			if err := json.Unmarshal([]byte(k.contextJSON), &m); err != nil {
				return nil, err
			}
			s, err := structpb.NewStruct(m)
			if err != nil {
				return nil, err
			}
			tk.Condition.Context = s
		}
	}
	return tk, nil
}
