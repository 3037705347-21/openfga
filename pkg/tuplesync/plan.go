package tuplesync

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// Rewrite is a same-identity condition change that must be applied as one
// transactional batch (delete old, write new).
type Rewrite struct {
	Delete *openfgav1.TupleKeyWithoutCondition
	Write  *openfgav1.TupleKey
}

// Plan is the fully validated reconciliation diff. When Blocked is true all
// action slices are nil by construction, so Apply can never make a partial
// change.
type Plan struct {
	StoreID        string
	Source         string
	ControlStoreID string
	ModelID        string

	Blocked bool
	Issues  []Issue

	AddClaims    []*Claim
	RemoveClaims []*Claim
	DataWrites   storage.Writes
	DataDeletes  storage.Deletes
	Rewrites     []Rewrite

	PlannedWrites   int
	PlannedDeletes  int
	PlannedRewrites int
}

// Planner builds Plans against a Backend. The zero value is not usable; create
// one with NewPlanner.
type Planner struct {
	b               Backend
	controlName     string
	readConcurrency int
}

// PlannerOption customizes a Planner.
type PlannerOption func(*Planner)

// WithControlStoreName overrides the default control-store lookup name.
func WithControlStoreName(name string) PlannerOption {
	return func(p *Planner) { p.controlName = name }
}

// WithReadConcurrency bounds the number of concurrent tuple/model reads.
func WithReadConcurrency(n int) PlannerOption {
	return func(p *Planner) { p.readConcurrency = n }
}

// NewPlanner creates a Planner.
func NewPlanner(b Backend, opts ...PlannerOption) *Planner {
	p := &Planner{
		b:               b,
		controlName:     DefaultControlStoreName,
		readConcurrency: 8,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Plan performs every read, static and model-level validation, and computes
// the convergence diff for the desired roster. It never writes.
func (p *Planner) Plan(ctx context.Context, roster *Roster) (*Plan, error) {
	plan := &Plan{StoreID: roster.StoreID, Source: roster.Source}

	targetModel, err := p.b.GetAuthorizationModel(ctx, roster.StoreID, roster.AuthorizationModelID)
	if err != nil {
		return nil, fmt.Errorf("read target authorization model: %w", err)
	}
	plan.ModelID = targetModel.GetId()

	controlID, err := EnsureControlStore(ctx, p.b, p.controlName, targetModel)
	if err != nil {
		return nil, err
	}
	plan.ControlStoreID = controlID

	// 1. Static roster validation. Malformed entries are excluded from the
	// remaining planning but every other check still runs, so a single Plan
	// surfaces all problems.
	staticIssues := roster.Validate()
	plan.Issues = append(plan.Issues, staticIssues...)

	type prepared struct {
		entry RosterEntry
		tk    *openfgav1.TupleKey
		valid bool
	}
	preparedEntries := make([]prepared, len(roster.Tuples))
	for i := range roster.Tuples {
		e := roster.Tuples[i]
		if msg := e.validate(); msg != "" {
			preparedEntries[i] = prepared{entry: e, valid: false}
			continue
		}
		tk, convErr := e.toTupleKey()
		if convErr != nil {
			plan.Issues = append(plan.Issues, Issue{
				Code: IssueMalformedTuple, TupleKey: e.keyString(), Message: convErr.Error(),
			})
			preparedEntries[i] = prepared{entry: e, valid: false}
			continue
		}
		preparedEntries[i] = prepared{entry: e, tk: tk, valid: true}
	}

	// 2. Model-level validation against the target model.
	ts, err := typesystem.New(targetModel)
	if err != nil {
		return nil, fmt.Errorf("parse target authorization model: %w", err)
	}
	var shapeValid []prepared
	modelValid := map[string]struct{}{}
	for _, pe := range preparedEntries {
		if !pe.valid {
			continue
		}
		shapeValid = append(shapeValid, pe)
		id := pe.entry.keyString()
		modelOK := true
		if mErr := validateTupleForModel(ts, pe.tk); mErr != nil {
			modelOK = false
			plan.Issues = append(plan.Issues, Issue{
				Code:     IssueModelValidation,
				TupleKey: id,
				Message:  fmt.Sprintf("tuple rejected by model %s: %v; fix the roster entry or update the authorization model", plan.ModelID, mErr),
			})
		}
		if size := proto.Size(pe.tk.GetCondition().GetContext()); size > config.DefaultWriteContextByteLimit {
			modelOK = false
			plan.Issues = append(plan.Issues, Issue{
				Code:     IssueModelValidation,
				TupleKey: id,
				Message: fmt.Sprintf("condition context size %d bytes exceeds limit %d bytes",
					size, config.DefaultWriteContextByteLimit),
			})
		}
		if modelOK {
			modelValid[id] = struct{}{}
		}
	}

	// 3. Load and decode the ownership ledger for the target store.
	ledger, ledgerIssues, err := p.loadLedger(ctx, controlID, roster.StoreID)
	if err != nil {
		return nil, err
	}
	plan.Issues = append(plan.Issues, ledgerIssues...)

	// 4. Existence checks for roster keys and every owned claim key.
	identities := make([]string, 0, len(shapeValid))
	rosterShapeValid := map[string]struct{}{}
	rosterModelValid := map[string]*openfgav1.TupleKey{}
	for _, pe := range shapeValid {
		id := pe.entry.keyString()
		rosterShapeValid[id] = struct{}{}
		if _, ok := modelValid[id]; !ok {
			continue // model_validation issue already reported; plan is blocked
		}
		if _, dup := rosterModelValid[id]; dup {
			continue // duplicate_in_roster already reported
		}
		rosterModelValid[id] = pe.tk
		identities = append(identities, id)
	}
	for _, claims := range ledger.byIdentity {
		if claims[0].Source != roster.Source || len(distinctOwners(claims)) > 1 {
			continue
		}
		id := identityOf(claims[0])
		if _, ok := rosterModelValid[id]; !ok {
			identities = append(identities, id)
		}
	}
	sort.Strings(identities)

	existing, err := parallelMap(ctx, p.readConcurrency, identities,
		func(ctx context.Context, id string) (*openfgav1.Tuple, error) {
			k := parseIdentity(id)
			tp, rErr := p.b.ReadTuple(ctx, roster.StoreID, k)
			if errors.Is(rErr, ErrTupleNotFound) {
				return nil, nil
			}
			return tp, rErr
		})
	if err != nil {
		return nil, err
	}
	exists := make(map[string]*openfgav1.Tuple, len(identities))
	for i, id := range identities {
		if existing[i] != nil {
			exists[id] = existing[i]
		}
	}

	// 5. Classification. Actions are accumulated in dedup sets and only
	// attached to the plan when there are zero blocking issues.
	addClaims := map[string]*Claim{}
	removeClaims := map[string]*Claim{}
	dataWrites := map[string]*openfgav1.TupleKey{}
	dataDeletes := map[string]*openfgav1.TupleKeyWithoutCondition{}
	rewrites := map[string]Rewrite{}

	desiredClaim := func(tk *openfgav1.TupleKey) *Claim {
		c, _ := newClaim(roster.StoreID, roster.Source, tk)
		return c
	}

	// Cross-identity ledger owner conflicts (same identity claimed by two
	// different sources via two different claim objects).
	for id, claims := range ledger.byIdentity {
		if len(distinctOwners(claims)) > 1 {
			plan.Issues = append(plan.Issues, Issue{
				Code:     IssueLedgerUnverifiable,
				TupleKey: id,
				Message: fmt.Sprintf("tuple %q is claimed by multiple sources %v; refuse to reconcile until the ownership conflict is resolved in the control store",
					id, distinctOwners(claims)),
			})
		}
	}

	classified := map[string]struct{}{}
	for _, pe := range shapeValid {
		id := pe.entry.keyString()
		if _, ok := modelValid[id]; !ok {
			continue
		}
		if _, done := classified[id]; done {
			continue
		}
		classified[id] = struct{}{}
		claims := ledger.byIdentity[id]
		actual := exists[id]

		switch {
		case len(claims) == 0:
			if actual != nil {
				plan.Issues = append(plan.Issues, Issue{
					Code: IssueUnclaimedExisting, TupleKey: id,
					Message: fmt.Sprintf(
						"tuple %q already exists in the target store without an ownership claim; it may be a manual grant or an untracked write, so it will not be overwritten; verify ownership and run 'adopt' to let source %q manage it, or remove it from the roster/manually delete it",
						id, roster.Source),
				})
				continue
			}
			c := desiredClaim(pe.tk)
			addClaims[c.hash()] = c
			dataWrites[id] = rosterModelValid[id]

		case len(distinctOwners(claims)) > 1:
			continue // reported above

		case claims[0].Source != roster.Source:
			plan.Issues = append(plan.Issues, Issue{
				Code: IssueForeignSource, TupleKey: id, Source: claims[0].Source,
				Message: fmt.Sprintf(
					"tuple %q is already managed by source %q; a tuple has a single owner; remove it from this roster or reconcile sources",
					id, claims[0].Source),
			})

		default: // owned by this source: converge condition + stale duplicate claims
			desired := desiredClaim(pe.tk)
			for _, c := range claims {
				if c.hash() != desired.hash() {
					removeClaims[c.hash()] = c
				}
			}
			desiredExists := false
			for _, c := range claims {
				if c.hash() == desired.hash() {
					desiredExists = true
				}
			}
			if !desiredExists {
				addClaims[desired.hash()] = desired
			}

			switch {
			case actual == nil:
				// crash-residual: claim exists (or is being rewritten) but
				// the data tuple never made it / was deleted.
				dataWrites[id] = pe.tk
			case actualMatchesRoster(actual, desired):
				// data already at desired content; nothing to write.
			default:
				rewrites[id] = Rewrite{
					Delete: tupleUtils.TupleKeyToTupleKeyWithoutCondition(pe.tk),
					Write:  pe.tk,
				}
			}
		}
	}

	// Owned claims whose tuple left the roster: delete data, release claims.
	for id, claims := range ledger.byIdentity {
		if _, inRoster := rosterShapeValid[id]; inRoster {
			continue
		}
		if len(distinctOwners(claims)) > 1 || claims[0].Source != roster.Source {
			continue
		}
		for _, c := range claims {
			removeClaims[c.hash()] = c
		}
		if exists[id] != nil {
			dataDeletes[id] = &openfgav1.TupleKeyWithoutCondition{
				Object: claims[0].key.Object, Relation: claims[0].key.Relation, User: claims[0].key.User,
			}
		}
		// if the data tuple is already gone (crash-residual) we only release
		// the claim.
	}

	sortIssues(plan.Issues)

	if len(plan.Issues) > 0 {
		plan.Blocked = true
		return plan, nil
	}

	plan.AddClaims = sortedClaims(addClaims)
	plan.RemoveClaims = sortedClaims(removeClaims)
	plan.DataWrites = sortedWrites(dataWrites)
	plan.DataDeletes = sortedDeletes(dataDeletes)
	plan.Rewrites = sortedRewrites(rewrites)
	plan.PlannedWrites = len(plan.DataWrites)
	plan.PlannedDeletes = len(plan.DataDeletes)
	plan.PlannedRewrites = len(plan.Rewrites)
	return plan, nil
}

// PlanAdopt validates explicit adoption requests: every named tuple must
// exist and have no owner claim. The resulting plan only adds claims; it never
// touches data tuples.
func (p *Planner) PlanAdopt(ctx context.Context, storeID, source string, entries []RosterEntry) (*Plan, error) {
	plan := &Plan{StoreID: storeID, Source: source}

	targetModel, err := p.b.GetAuthorizationModel(ctx, storeID, "")
	if err != nil {
		return nil, fmt.Errorf("read target authorization model: %w", err)
	}
	plan.ModelID = targetModel.GetId()
	controlID, err := EnsureControlStore(ctx, p.b, p.controlName, targetModel)
	if err != nil {
		return nil, err
	}
	plan.ControlStoreID = controlID

	// Static shape validation for the adoption list.
	seen := map[string]struct{}{}
	var valid []RosterEntry
	for _, e := range entries {
		id := e.keyString()
		if msg := e.validate(); msg != "" {
			plan.Issues = append(plan.Issues, Issue{Code: IssueMalformedTuple, TupleKey: id, Message: msg})
			continue
		}
		if _, dup := seen[id]; dup {
			plan.Issues = append(plan.Issues, Issue{Code: IssueDuplicateInRoster, TupleKey: id,
				Message: "tuple is listed more than once in the adopt request"})
			continue
		}
		seen[id] = struct{}{}
		valid = append(valid, e)
	}

	ledger, ledgerIssues, err := p.loadLedger(ctx, controlID, storeID)
	if err != nil {
		return nil, err
	}
	plan.Issues = append(plan.Issues, ledgerIssues...)

	addClaims := map[string]*Claim{}
	actuals, err := parallelMap(ctx, p.readConcurrency, valid,
		func(ctx context.Context, e RosterEntry) (*openfgav1.Tuple, error) {
			tp, rErr := p.b.ReadTuple(ctx, storeID, &openfgav1.TupleKeyWithoutCondition{
				Object: e.Object, Relation: e.Relation, User: e.User,
			})
			if errors.Is(rErr, ErrTupleNotFound) {
				return nil, nil
			}
			return tp, rErr
		})
	if err != nil {
		return nil, err
	}

	for i, e := range valid {
		id := e.keyString()
		tp := actuals[i]
		if tp == nil {
			plan.Issues = append(plan.Issues, Issue{Code: IssueAdoptNotFound, TupleKey: id,
				Message: "cannot adopt a tuple that does not exist in the target store; write it first or remove it from the adopt request"})
			continue
		}
		if claims, ok := ledger.byIdentity[id]; ok && len(claims) > 0 {
			plan.Issues = append(plan.Issues, Issue{
				Code: IssueAdoptAlreadyClaimed, TupleKey: id, Source: claims[0].Source,
				Message: fmt.Sprintf("tuple is already claimed (by source %q); adoption would steal ownership and is refused", claims[0].Source),
			})
			continue
		}
		// Claim the tuple exactly as it currently exists (incl. condition).
		c, cErr := newClaim(storeID, source, tp.GetKey())
		if cErr != nil {
			plan.Issues = append(plan.Issues, Issue{Code: IssueMalformedTuple, TupleKey: id, Message: cErr.Error()})
			continue
		}
		addClaims[c.hash()] = c
	}

	sortIssues(plan.Issues)
	if len(plan.Issues) > 0 {
		plan.Blocked = true
		return plan, nil
	}
	plan.AddClaims = sortedClaims(addClaims)
	return plan, nil
}

// ledgerView is the decoded set of claims in one target store.
type ledgerView struct {
	byID       map[string]*Claim
	byIdentity map[string][]*Claim
}

func (p *Planner) loadLedger(ctx context.Context, controlID, targetStoreID string) (*ledgerView, []Issue, error) {
	view := &ledgerView{byID: map[string]*Claim{}, byIdentity: map[string][]*Claim{}}
	var issues []Issue
	hashes := map[string]struct{}{}

	// Every claim carries exactly one target tuple; reading by
	// fga_store:<target> enumerates all claims of this target store, for
	// every source.
	err := p.b.ReadByUser(ctx, controlID, controlStoreType+":"+targetStoreID, func(tp *openfgav1.Tuple) error {
		k := tp.GetKey()
		if k.GetRelation() != relTarget || !isClaimObject(k.GetObject()) {
			issues = append(issues, Issue{
				Code: IssueLedgerUnverifiable, TupleKey: k.GetObject(),
				Message: fmt.Sprintf("unexpected ledger tuple %q on control-store target index", k.String()),
			})
			return nil
		}
		hashes[claimHashFromObject(k.GetObject())] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	hashList := make([]string, 0, len(hashes))
	for h := range hashes {
		hashList = append(hashList, h)
	}
	sort.Strings(hashList)

	type decoded struct {
		obj   string
		claim *Claim
		err   error
	}
	results, err := parallelMap(ctx, p.readConcurrency, hashList,
		func(ctx context.Context, h string) (decoded, error) {
			obj := controlClaimType + ":" + h
			var tuples []*openfgav1.Tuple
			rErr := p.b.ReadObject(ctx, controlID, obj, func(tp *openfgav1.Tuple) error {
				tuples = append(tuples, tp)
				return nil
			})
			if rErr != nil {
				return decoded{}, rErr
			}
			c, dErr := decodeClaimObject(obj, tuples)
			return decoded{obj: obj, claim: c, err: dErr}, nil
		})
	if err != nil {
		return nil, nil, err
	}

	for _, r := range results {
		if r.err != nil {
			issues = append(issues, Issue{
				Code: IssueLedgerUnverifiable, TupleKey: r.obj,
				Message: r.err.Error(),
			})
			continue
		}
		c := r.claim
		view.byID[c.hash()] = c
		id := identityOf(c)
		view.byIdentity[id] = append(view.byIdentity[id], c)
	}

	for id := range view.byIdentity {
		sort.Slice(view.byIdentity[id], func(a, b int) bool {
			return view.byIdentity[id][a].hash() < view.byIdentity[id][b].hash()
		})
	}
	sortIssues(issues)
	return view, issues, nil
}

func actualMatchesRoster(actual *openfgav1.Tuple, desired *Claim) bool {
	got, err := newClaim(desired.StoreID, desired.Source, actual.GetKey())
	if err != nil {
		return false
	}
	return got.hash() == desired.hash()
}

func identityOf(c *Claim) string {
	return c.key.Object + "#" + c.key.Relation + "@" + c.key.User
}

func distinctOwners(claims []*Claim) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, c := range claims {
		if _, ok := seen[c.Source]; !ok {
			seen[c.Source] = struct{}{}
			out = append(out, c.Source)
		}
	}
	sort.Strings(out)
	return out
}

func isClaimObject(object string) bool {
	_, ok := claimHashFromObjectOK(object)
	return ok
}

func claimHashFromObject(object string) string {
	h, _ := claimHashFromObjectOK(object)
	return h
}

func claimHashFromObjectOK(object string) (string, bool) {
	return strings.CutPrefix(object, controlClaimType+":")
}

func parseIdentity(id string) *openfgav1.TupleKeyWithoutCondition {
	// object#relation@user; user may itself contain '#' (userset) but not '@'.
	at := -1
	for i, r := range id {
		if r == '@' {
			at = i
			break
		}
	}
	if at < 0 {
		return &openfgav1.TupleKeyWithoutCondition{}
	}
	or := id[:at]
	for i, r := range or {
		if r == '#' {
			return &openfgav1.TupleKeyWithoutCondition{
				Object:   or[:i],
				Relation: or[i+1:],
				User:     id[at+1:],
			}
		}
	}
	return &openfgav1.TupleKeyWithoutCondition{User: id[at+1:]}
}

func sortedClaims(in map[string]*Claim) []*Claim {
	out := make([]*Claim, 0, len(in))
	for _, c := range in {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].hash() < out[j].hash() })
	return out
}

func sortedWrites(in map[string]*openfgav1.TupleKey) storage.Writes {
	out := make(storage.Writes, 0, len(in))
	for _, tk := range in {
		out = append(out, tk)
	}
	sort.Slice(out, func(i, j int) bool { return tupleUtils.TupleKeyToString(out[i]) < tupleUtils.TupleKeyToString(out[j]) })
	return out
}

func sortedDeletes(in map[string]*openfgav1.TupleKeyWithoutCondition) storage.Deletes {
	out := make(storage.Deletes, 0, len(in))
	for _, tk := range in {
		out = append(out, tk)
	}
	sort.Slice(out, func(i, j int) bool { return tupleUtils.TupleKeyToString(out[i]) < tupleUtils.TupleKeyToString(out[j]) })
	return out
}

func sortedRewrites(in map[string]Rewrite) []Rewrite {
	out := make([]Rewrite, 0, len(in))
	for _, r := range in {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return tupleUtils.TupleKeyToString(out[i].Delete) < tupleUtils.TupleKeyToString(out[j].Delete)
	})
	return out
}
