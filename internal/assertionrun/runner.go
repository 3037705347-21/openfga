// Package assertionrun runs batches of authorization assertions against a
// pinned, immutable authorization state.
//
// Every batch fixes, before any assertion is evaluated:
//
//   - the authorization model, and
//   - the relationship change position (the newest changelog ULID in scope).
//
// Tuple reads for that batch are served from a throwaway store rebuilt by
// replaying the changelog up to the pinned ULID, so a run, a resume after an
// interruption and a full replay all observe the exact same permission state
// and can never mix in tuples written later. Per-assertion results are
// persisted as the batch progresses, enabling continuation into a single
// completion report. Two completed batches can be diffed to see exactly which
// assertions changed outcome.
//
// The legacy assertions read/write interfaces are untouched: callers source
// assertions however they wish (typically via ReadAssertions) and hand them to
// [Runner.Start].
package assertionrun

import (
	"context"
	"errors"
	"time"

	"github.com/oklog/ulid/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/assertionrun"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
)

// replayPageSize is the changelog page size used when rebuilding the tuple
// state for a boundary. It only affects round-trip counts, not correctness.
const replayPageSize = 200

// Common runner errors.
var (
	// ErrNoAssertions is returned when a batch is started without assertions.
	ErrNoAssertions = errors.New("assertion run batch must contain at least one assertion")

	// ErrAuthorizationModelNotFound is returned when the requested (or latest)
	// authorization model does not exist in the store.
	ErrAuthorizationModelNotFound = errors.New("authorization model not found")
)

// ModelReader loads authorization models. [storage.OpenFGADatastore] already
// satisfies it.
type ModelReader interface {
	ReadAuthorizationModel(ctx context.Context, store, id string) (*openfgav1.AuthorizationModel, error)
	FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error)
}

// RunMode controls how [Runner.Run] treats already-stored results.
type RunMode int

const (
	// ModeContinue finishes an interrupted run: assertions with a definitive
	// stored result are reused and previously errored assertions are retried.
	ModeContinue RunMode = iota
	// ModeReplay re-evaluates every assertion. The pinned model and change
	// boundary are never moved; only the results are refreshed.
	ModeReplay
)

// CheckInput carries everything an assertion evaluator needs.
type CheckInput struct {
	StoreID   string
	Typesys   *typesystem.TypeSystem
	Reader    storage.RelationshipTupleReader
	Assertion *openfgav1.Assertion
}

// CheckFunc evaluates a single assertion and reports whether it was allowed.
type CheckFunc func(ctx context.Context, in CheckInput) (allowed bool, err error)

// Option customizes a Runner.
type Option func(*Runner)

// WithCheckFunc overrides the default graph-based evaluator. It is mainly
// useful for tests that need deterministic or fault-injecting evaluations.
func WithCheckFunc(fn CheckFunc) Option {
	return func(r *Runner) {
		r.check = fn
	}
}

// Runner orchestrates assertion run batches.
type Runner struct {
	models  ModelReader
	changes storage.ChangelogBackend
	ledger  assertionrun.LedgerBackend

	check CheckFunc
	close graph.CheckResolverCloser

	// newSeed returns a throwaway datastore used to hold the tuples visible at
	// a pinned boundary. Overridable in tests.
	newSeed func() storage.OpenFGADatastore
}

// New builds a Runner backed by the given model reader, changelog and ledger.
// By default assertions are evaluated by the production graph checker.
func New(models ModelReader, changes storage.ChangelogBackend, ledger assertionrun.LedgerBackend, opts ...Option) (*Runner, error) {
	r := &Runner{
		models:  models,
		changes: changes,
		ledger:  ledger,
		newSeed: func() storage.OpenFGADatastore { return memory.New() },
	}

	for _, opt := range opts {
		opt(r)
	}

	if r.check == nil {
		resolver, closeResolvers, err := graph.NewOrderedCheckResolvers().Build()
		if err != nil {
			return nil, err
		}
		ev := &graphEvaluator{resolver: resolver}
		r.check = ev.check
		// Keep the closer reachable only through a Close on the runner.
		r.close = closeResolvers
	}

	return r, nil
}

// Close releases resources held by the default evaluator. It is a no-op when a
// custom check function was supplied.
func (r *Runner) Close() {
	if r.close != nil {
		r.close()
		r.close = nil
	}
}

// graphEvaluator evaluates assertions with the real local check resolver.
type graphEvaluator struct {
	resolver graph.CheckResolver
}

func (g *graphEvaluator) check(ctx context.Context, in CheckInput) (bool, error) {
	cmd := commands.NewCheckCommand(in.Reader, g.resolver, in.Typesys)

	tk := in.Assertion.GetTupleKey()
	res, err := cmd.Execute(ctx, &commands.CheckCommandParams{
		StoreID: in.StoreID,
		TupleKey: &openfgav1.CheckRequestTupleKey{
			Object:   tk.GetObject(),
			Relation: tk.GetRelation(),
			User:     tk.GetUser(),
		},
		ContextualTuples: &openfgav1.ContextualTupleKeys{
			TupleKeys: in.Assertion.GetContextualTuples(),
		},
		Context: in.Assertion.GetContext(),
	})
	if err != nil {
		return false, err
	}

	return res.Allowed, nil
}

// StartRequest configures a new assertion run batch.
type StartRequest struct {
	StoreID    string
	ModelID    string // optional; resolved to the latest model when empty
	Name       string
	Assertions []*openfgav1.Assertion
}

// Start pins the model and current relationship change position and records a
// new RUNNING batch with an immutable copy of the assertions.
func (r *Runner) Start(ctx context.Context, req StartRequest) (*assertionrun.Batch, error) {
	if len(req.Assertions) == 0 {
		return nil, ErrNoAssertions
	}

	model, err := r.resolveModel(ctx, req.StoreID, req.ModelID)
	if err != nil {
		return nil, err
	}

	// Fail fast if the pinned model cannot be used for checks.
	if _, err := typesystem.New(model); err != nil {
		return nil, err
	}

	headULID, err := r.currentHeadULID(ctx, req.StoreID)
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	batch := &assertionrun.Batch{
		ID:      ulid.Make().String(),
		StoreID: req.StoreID,
		Name:    req.Name,
		Boundary: assertionrun.Boundary{
			StoreID:              req.StoreID,
			AuthorizationModelID: model.GetId(),
			ChangeULID:           headULID,
			CapturedAt:           now,
		},
		Status:               assertionrun.StatusRunning,
		TotalAssertions:      len(req.Assertions),
		AssertionFingerprint: assertionrun.FingerprintAssertions(req.Assertions),
		CreatedAt:            now,
		UpdatedAt:            now,
	}

	return r.ledger.InitBatch(ctx, batch, req.Assertions)
}

// Run executes the batch. In [ModeContinue] an already-completed batch is
// returned as-is; in [ModeReplay] every assertion is re-evaluated while the
// boundary is preserved. An interrupted Run leaves persisted results behind
// and a later ModeContinue finishes into a single completion report.
func (r *Runner) Run(ctx context.Context, store, batchID string, mode RunMode) (*assertionrun.Report, error) {
	batch, err := r.ledger.GetBatch(ctx, store, batchID)
	if err != nil {
		return nil, err
	}

	if mode == ModeContinue && batch.Status == assertionrun.StatusCompleted {
		return r.Report(ctx, store, batchID)
	}

	pinned, err := r.ledger.GetAssertions(ctx, store, batchID)
	if err != nil {
		return nil, err
	}

	model, err := r.models.ReadAuthorizationModel(ctx, store, batch.Boundary.AuthorizationModelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrAuthorizationModelNotFound
		}
		return nil, err
	}
	typesys, err := typesystem.New(model)
	if err != nil {
		return nil, err
	}

	// The reader only ever exposes tuples visible at the pinned boundary.
	reader, err := r.boundedReader(ctx, store, batch.Boundary)
	if err != nil {
		return nil, err
	}

	stored, err := r.ledger.GetResults(ctx, store, batchID)
	if err != nil {
		return nil, err
	}
	existing := make(map[int]*assertionrun.Result, len(stored))
	for _, res := range stored {
		existing[res.Index] = res
	}

	// A replay of a finished batch flips it back to RUNNING for its duration.
	if batch.Status == assertionrun.StatusCompleted {
		batch, err = r.ledger.UpdateBatch(ctx, store, batchID, func(b *assertionrun.Batch) error {
			b.Status = assertionrun.StatusRunning
			b.CompletedAt = time.Time{}
			b.UpdatedAt = time.Now().UTC()
			return nil
		})
		if err != nil {
			return nil, err
		}
	}

	for i, a := range pinned {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if prev := existing[i]; mode == ModeContinue && prev != nil && prev.Status != assertionrun.ResultError {
			continue
		}

		attempts := 1
		if prev := existing[i]; prev != nil {
			attempts = prev.Attempts + 1
		}

		started := time.Now()
		allowed, checkErr := r.check(ctx, CheckInput{
			StoreID:   store,
			Typesys:   typesys,
			Reader:    reader,
			Assertion: a,
		})
		duration := time.Since(started)

		result := &assertionrun.Result{
			BatchID:    batchID,
			Index:      i,
			Assertion:  a,
			Expected:   a.GetExpectation(),
			Got:        allowed,
			Status:     statusFor(a.GetExpectation(), allowed, checkErr),
			Attempts:   attempts,
			DurationMS: duration.Milliseconds(),
			UpdatedAt:  time.Now().UTC(),
		}
		if checkErr != nil {
			result.Error = checkErr.Error()
			// An error on this assertion is still a persisted, resumable result,
			// but a cancelled/expired context aborts the whole batch.
			if errors.Is(checkErr, context.Canceled) || errors.Is(checkErr, context.DeadlineExceeded) {
				_ = r.ledger.PutResult(ctx, store, batchID, result)
				return nil, checkErr
			}
		}

		if err := r.ledger.PutResult(ctx, store, batchID, result); err != nil {
			return nil, err
		}

		if _, err := r.ledger.UpdateBatch(ctx, store, batchID, func(b *assertionrun.Batch) error {
			b.UpdatedAt = time.Now().UTC()
			return nil
		}); err != nil {
			return nil, err
		}
	}

	completedAt := time.Now().UTC()
	batch, err = r.ledger.UpdateBatch(ctx, store, batchID, func(b *assertionrun.Batch) error {
		b.Status = assertionrun.StatusCompleted
		b.CompletedAt = completedAt
		b.UpdatedAt = completedAt
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.Report(ctx, store, batchID)
}

// Continue resumes an interrupted (or completes an in-flight) batch.
func (r *Runner) Continue(ctx context.Context, store, batchID string) (*assertionrun.Report, error) {
	return r.Run(ctx, store, batchID, ModeContinue)
}

// Replay re-evaluates every assertion of a batch while reusing its original
// model and change boundary.
func (r *Runner) Replay(ctx context.Context, store, batchID string) (*assertionrun.Report, error) {
	return r.Run(ctx, store, batchID, ModeReplay)
}

// Report assembles the current report of a batch. A completed batch carries a
// completion snapshot; a still-running batch carries partial results.
func (r *Runner) Report(ctx context.Context, store, batchID string) (*assertionrun.Report, error) {
	batch, err := r.ledger.GetBatch(ctx, store, batchID)
	if err != nil {
		return nil, err
	}
	results, err := r.ledger.GetResults(ctx, store, batchID)
	if err != nil {
		return nil, err
	}

	report := &assertionrun.Report{Batch: batch, Results: results}
	if batch.Status == assertionrun.StatusCompleted {
		report.Snapshot = buildSnapshot(batch, results)
	}
	return report, nil
}

// resolveModel loads the explicitly requested model, or the latest when
// modelID is empty.
func (r *Runner) resolveModel(ctx context.Context, store, modelID string) (*openfgav1.AuthorizationModel, error) {
	if modelID == "" {
		model, err := r.models.FindLatestAuthorizationModel(ctx, store)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrAuthorizationModelNotFound
			}
			return nil, err
		}
		return model, nil
	}

	model, err := r.models.ReadAuthorizationModel(ctx, store, modelID)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, ErrAuthorizationModelNotFound
		}
		return nil, err
	}
	return model, nil
}

// currentHeadULID returns the ULID of the newest changelog entry, or "" when
// the changelog is empty.
func (r *Runner) currentHeadULID(ctx context.Context, store string) (string, error) {
	changes, cont, err := r.changes.ReadChanges(ctx, store, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(1, ""),
		SortDesc:   true,
	})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return "", nil
		}
		return "", err
	}
	if len(changes) == 0 {
		return "", nil
	}
	return cont, nil
}

// boundedReader rebuilds the set of tuples visible at boundary by replaying
// the changelog in ascending order up to and including boundary.ChangeULID,
// then serves reads from an isolated throwaway store. Tuples written after the
// boundary are never read.
func (r *Runner) boundedReader(ctx context.Context, store string, boundary assertionrun.Boundary) (storage.RelationshipTupleReader, error) {
	seed := r.newSeed()

	// Empty boundary: nothing had been written when the batch was pinned.
	if boundary.ChangeULID == "" {
		return seed, nil
	}

	state := make(map[string]*openfgav1.TupleKey)

	from := ""
	for {
		changes, cont, err := r.readChangesAsc(ctx, store, replayPageSize, from)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				break
			}
			return nil, err
		}
		if len(changes) == 0 {
			break
		}

		if ulidLessOrEqual(cont, boundary.ChangeULID) {
			// The entire page is within the boundary.
			for _, c := range changes {
				applyChange(state, c)
			}
			from = cont
			if len(changes) < replayPageSize {
				break
			}
			continue
		}

		// The boundary falls inside this page. Re-walk the same range one row
		// at a time so each change can be paired with its own ULID, stopping at
		// the first change newer than the boundary.
		tailFrom := from
		for {
			one, oneCont, err := r.readChangesAsc(ctx, store, 1, tailFrom)
			if err != nil {
				if errors.Is(err, storage.ErrNotFound) {
					goto replayed
				}
				return nil, err
			}
			if len(one) == 0 || !ulidLessOrEqual(oneCont, boundary.ChangeULID) {
				goto replayed
			}
			applyChange(state, one[0])
			tailFrom = oneCont
		}
	}

replayed:
	tuples := make([]*openfgav1.TupleKey, 0, len(state))
	for _, tk := range state {
		tuples = append(tuples, tk)
	}

	maxBatch := seed.MaxTuplesPerWrite()
	for start := 0; start < len(tuples); start += maxBatch {
		end := start + maxBatch
		if end > len(tuples) {
			end = len(tuples)
		}
		if err := seed.Write(ctx, store, nil, tuples[start:end]); err != nil {
			return nil, err
		}
	}

	return seed, nil
}

func (r *Runner) readChangesAsc(ctx context.Context, store string, pageSize int, from string) ([]*openfgav1.TupleChange, string, error) {
	return r.changes.ReadChanges(ctx, store, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(int32(pageSize), from),
	})
}

// applyChange folds one ordered changelog entry into the materialized tuple set.
func applyChange(state map[string]*openfgav1.TupleKey, change *openfgav1.TupleChange) {
	tk := change.GetTupleKey()
	key := tk.GetObject() + "\x00" + tk.GetRelation() + "\x00" + tk.GetUser()

	switch change.GetOperation() {
	case openfgav1.TupleOperation_TUPLE_OPERATION_WRITE:
		state[key] = tk
	case openfgav1.TupleOperation_TUPLE_OPERATION_DELETE:
		delete(state, key)
	}
}

// ulidLessOrEqual compares two ULIDs, accepting lexicographic comparison as a
// fallback for malformed tokens (valid ULIDs sort correctly either way).
func ulidLessOrEqual(a, b string) bool {
	ua, errA := ulid.Parse(a)
	ub, errB := ulid.Parse(b)
	if errA == nil && errB == nil {
		return ua.Compare(ub) <= 0
	}
	return a <= b
}

func statusFor(expected, allowed bool, err error) assertionrun.ResultStatus {
	switch {
	case err != nil:
		return assertionrun.ResultError
	case expected == allowed:
		return assertionrun.ResultMatched
	default:
		return assertionrun.ResultMismatched
	}
}

// buildSnapshot derives the single completion snapshot from the finished batch
// and its per-assertion results.
func buildSnapshot(batch *assertionrun.Batch, results []*assertionrun.Result) *assertionrun.CompletionSnapshot {
	snap := &assertionrun.CompletionSnapshot{
		BatchID:         batch.ID,
		StoreID:         batch.StoreID,
		Boundary:        batch.Boundary,
		Status:          batch.Status,
		TotalAssertions: batch.TotalAssertions,
		Fingerprint:     assertionrun.FingerprintResults(results),
		StartedAt:       batch.CreatedAt,
		CompletedAt:     batch.CompletedAt,
		DurationMS:      batch.CompletedAt.Sub(batch.CreatedAt).Milliseconds(),
	}

	for _, res := range results {
		switch res.Status {
		case assertionrun.ResultMatched:
			snap.Matched++
		case assertionrun.ResultMismatched:
			snap.Mismatched++
		case assertionrun.ResultError:
			snap.Errored++
		}
	}

	return snap
}
