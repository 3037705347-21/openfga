// Package tuplesync reconciles a roster of explicit OpenFGA relationship
// tuples ("the team permission list") against one target OpenFGA store, while
// protecting tuples that were not written by the roster source.
//
// The guarantees provided by a successful Sync are:
//
//   - The set of tuples managed by the source is exactly the current roster
//     (adds, removals, role changes and condition changes all converge).
//   - Tuples without an ownership claim in the control store are treated as
//     manually managed and are never deleted or rewritten.
//   - Validation and conflict detection all happen before any write: if the
//     roster contains duplicates, malformed tuples, model violations, or if
//     ownership of an existing tuple cannot be confirmed, the whole run takes
//     no effect and every blocking issue is reported.
//   - The claim-first / delete-data-before-release-claim protocol makes every
//     interrupted run recoverable: rerunning Sync after a mid-flight failure
//     or a process restart converges to the current roster.
package tuplesync

// IssueCode identifies a blocking problem found while planning a sync or
// adopt run.
type IssueCode string

const (
	// IssueMalformedRoster means the roster envelope itself is invalid
	// (missing store/source, empty tuple list, invalid JSON document shape).
	IssueMalformedRoster IssueCode = "malformed_roster"

	// IssueDuplicateInRoster means the same tuple identity
	// (object#relation@user) appears more than once in the roster, regardless
	// of whether its condition differs.
	IssueDuplicateInRoster IssueCode = "duplicate_in_roster"

	// IssueMalformedTuple means a roster entry fails structural validation
	// (missing fields, illegal characters, invalid condition shape).
	IssueMalformedTuple IssueCode = "malformed_tuple"

	// IssueModelValidation means a roster entry would be rejected by the
	// target store's authorization model (unknown type/relation, wrong user
	// type, unknown condition, oversized context, implicit tuple, ...).
	IssueModelValidation IssueCode = "model_validation"

	// IssueUnclaimedExisting means the roster wants a tuple key that already
	// exists in the target store, but no ownership claim exists for it: it
	// cannot be told apart from a manual grant. It must be resolved by the
	// operator (adopt it, remove it from the roster, or delete it manually).
	IssueUnclaimedExisting IssueCode = "unclaimed_existing"

	// IssueForeignSource means the tuple key is already claimed by a
	// different source.
	IssueForeignSource IssueCode = "foreign_source"

	// IssueLedgerUnverifiable means the ownership ledger itself cannot be
	// trusted in its current state (multiple owners for one claim, missing or
	// corrupt payload chunks, control-store model drift, ...).
	IssueLedgerUnverifiable IssueCode = "ledger_unverifiable"

	// IssueAdoptNotFound means an adopt request names a tuple that does not
	// exist in the target store.
	IssueAdoptNotFound IssueCode = "adopt_not_found"

	// IssueAdoptAlreadyClaimed means an adopt request names a tuple that
	// already has an owner claim (by this or another source).
	IssueAdoptAlreadyClaimed IssueCode = "adopt_already_claimed"
)

// Issue is a single, actionable blocking problem. Every field is safe to
// serialize to JSON and print to operators; it never contains credentials.
type Issue struct {
	// Code is the machine-readable IssueCode.
	Code IssueCode `json:"code"`
	// TupleKey is the full "object#relation@user" string the issue pertains
	// to (empty for envelope-level issues).
	TupleKey string `json:"tuple_key,omitempty"`
	// Source is the owning source recorded in the ledger, when knowable.
	Source string `json:"source,omitempty"`
	// Message explains the problem and the suggested next step.
	Message string `json:"message"`
}
