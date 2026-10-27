package assertionrun_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	runsvc "github.com/openfga/openfga/internal/assertionrun"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/server/commands"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/assertionrun"
	ledgermem "github.com/openfga/openfga/pkg/storage/assertionrun/memory"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

const testModel = `
model
  schema 1.1

type user

type document
  relations
    define viewer: [user]
`

func bootstrapStore(t *testing.T) (storage.OpenFGADatastore, string) {
	t.Helper()

	ds := memory.New()
	t.Cleanup(ds.Close)

	storeID := ulid.Make().String()
	model := testutils.MustTransformDSLToProtoWithID(testModel)
	require.NoError(t, ds.WriteAuthorizationModel(context.Background(), storeID, model))

	return ds, storeID
}

func writeTuples(t *testing.T, ds storage.OpenFGADatastore, storeID string, tupleStrs ...string) {
	t.Helper()
	tks := tupleUtils.MustParseTupleStrings(tupleStrs...)
	require.NoError(t, ds.Write(context.Background(), storeID, nil, tks))
}

func deleteTuple(t *testing.T, ds storage.OpenFGADatastore, storeID, object, relation, user string) {
	t.Helper()
	require.NoError(t, ds.Write(context.Background(), storeID,
		[]*openfgav1.TupleKeyWithoutCondition{{Object: object, Relation: relation, User: user}}, nil))
}

func assertion(object, relation, user string, expect bool) *openfgav1.Assertion {
	return &openfgav1.Assertion{
		TupleKey:    tupleUtils.NewAssertionTupleKey(object, relation, user),
		Expectation: expect,
	}
}

func newRunner(t *testing.T, ds storage.OpenFGADatastore, opts ...runsvc.Option) *runsvc.Runner {
	t.Helper()
	r, err := runsvc.New(ds, ds, ledgermem.New(), opts...)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r
}

func resultByUser(report *assertionrun.Report, user string) *assertionrun.Result {
	for _, r := range report.Results {
		if r.Assertion.GetTupleKey().GetUser() == user {
			return r
		}
	}
	return nil
}

// A batch evaluates strictly against the pinned boundary: tuples written or
// deleted afterwards cannot leak into the report.
func TestRunPinsRelationshipBoundary(t *testing.T) {
	ds, storeID := bootstrapStore(t)
	writeTuples(t, ds, storeID, "document:1#viewer@user:anne")

	runner := newRunner(t, ds)

	batch, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID: storeID,
		Assertions: []*openfgav1.Assertion{
			assertion("document:1", "viewer", "user:anne", true),   // present at boundary
			assertion("document:1", "viewer", "user:carol", false), // added after boundary
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, batch.Boundary.AuthorizationModelID)
	require.NotEmpty(t, batch.Boundary.ChangeULID)
	require.Equal(t, assertionrun.StatusRunning, batch.Status)

	// Mutate the live data after the boundary was pinned.
	writeTuples(t, ds, storeID, "document:1#viewer@user:carol")
	deleteTuple(t, ds, storeID, "document:1", "viewer", "user:anne")

	report, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, report.Batch.Status)
	require.NotNil(t, report.Snapshot)

	anne := resultByUser(report, "user:anne")
	carol := resultByUser(report, "user:carol")
	require.Equal(t, assertionrun.ResultMatched, anne.Status, "anne must still be allowed from the pinned state")
	require.True(t, anne.Got)
	require.Equal(t, assertionrun.ResultMatched, carol.Status, "carol's later write must be invisible")
	require.False(t, carol.Got)

	require.Equal(t, 2, report.Snapshot.TotalAssertions)
	require.Equal(t, 2, report.Snapshot.Matched)
	require.Equal(t, 0, report.Snapshot.Mismatched)
	require.Equal(t, 0, report.Snapshot.Errored)
}

// An interrupted run persists per-assertion progress; a later Continue retries
// previously errored assertions, skips finished ones and yields one snapshot.
func TestInterruptedRunContinuesIntoSingleReport(t *testing.T) {
	ds, storeID := bootstrapStore(t)
	writeTuples(t, ds, storeID,
		"document:1#viewer@user:a0",
		"document:1#viewer@user:a1",
		"document:1#viewer@user:a2",
		"document:1#viewer@user:a3",
	)

	// Build a real graph-backed evaluator and wrap it with faults.
	resolver, closeResolvers, err := graph.NewOrderedCheckResolvers().Build()
	require.NoError(t, err)
	t.Cleanup(closeResolvers)

	real := func(ctx context.Context, in runsvc.CheckInput) (bool, error) {
		tk := in.Assertion.GetTupleKey()
		cmd := commands.NewCheckCommand(in.Reader, resolver, in.Typesys)
		res, err := cmd.Execute(ctx, &commands.CheckCommandParams{
			StoreID:          storeID,
			TupleKey:         tupleUtils.NewCheckRequestTupleKey(tk.GetObject(), tk.GetRelation(), tk.GetUser()),
			ContextualTuples: &openfgav1.ContextualTupleKeys{TupleKeys: in.Assertion.GetContextualTuples()},
			Context:          in.Assertion.GetContext(),
		})
		if err != nil {
			return false, err
		}
		return res.Allowed, nil
	}

	var mu sync.Mutex
	transientDone := false
	var calls int
	runCtx, cancel := context.WithCancel(context.Background())

	faulty := func(ctx context.Context, in runsvc.CheckInput) (bool, error) {
		user := in.Assertion.GetTupleKey().GetUser()
		idx, err := strconv.Atoi(strings.TrimPrefix(user, "user:a"))
		require.NoError(t, err)

		mu.Lock()
		calls++
		// a1 fails transiently on its first attempt only.
		fail := idx == 1 && !transientDone
		if idx == 1 {
			transientDone = true
		}
		shouldCancel := calls >= 3
		mu.Unlock()

		if fail {
			return false, errors.New("transient evaluator failure")
		}

		allowed, err := real(ctx, in)

		// Cancel the whole run after the third evaluator call so it aborts
		// before reaching a3, leaving an interrupted RUNNING batch.
		if shouldCancel {
			cancel()
		}
		return allowed, err
	}

	runner := newRunner(t, ds, runsvc.WithCheckFunc(faulty))

	batch, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID: storeID,
		Assertions: []*openfgav1.Assertion{
			assertion("document:1", "viewer", "user:a0", true),
			assertion("document:1", "viewer", "user:a1", true),
			assertion("document:1", "viewer", "user:a2", true),
			assertion("document:1", "viewer", "user:a3", true),
		},
	})
	require.NoError(t, err)

	_, err = runner.Run(runCtx, storeID, batch.ID, runsvc.ModeContinue)
	require.ErrorIs(t, err, context.Canceled)

	partial, err := runner.Report(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusRunning, partial.Batch.Status)
	require.Nil(t, partial.Snapshot)
	require.Len(t, partial.Results, 3)
	require.Equal(t, assertionrun.ResultError, resultByUser(partial, "user:a1").Status)

	// Resume on a fresh context.
	final, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, final.Batch.Status)
	require.Len(t, final.Results, 4)

	a1 := resultByUser(final, "user:a1")
	require.Equal(t, assertionrun.ResultMatched, a1.Status)
	require.Equal(t, 2, a1.Attempts, "the errored assertion must be retried")
	for _, user := range []string{"user:a0", "user:a2", "user:a3"} {
		require.Equal(t, assertionrun.ResultMatched, resultByUser(final, user).Status)
	}

	require.Equal(t, 4, final.Snapshot.Matched)
	require.Equal(t, 0, final.Snapshot.Errored)
}

// Replaying a finished batch reuses the original model and change boundary even
// after more writes, so the outcomes and fingerprint stay identical.
func TestReplayPreservesBoundaryAndOutcomes(t *testing.T) {
	ds, storeID := bootstrapStore(t)
	writeTuples(t, ds, storeID, "document:1#viewer@user:anne")

	runner := newRunner(t, ds)

	batch, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID: storeID,
		Assertions: []*openfgav1.Assertion{
			assertion("document:1", "viewer", "user:anne", true),
			assertion("document:1", "viewer", "user:carol", false),
		},
	})
	require.NoError(t, err)

	first, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)

	// Data changes after the first completion must not move the boundary.
	writeTuples(t, ds, storeID, "document:1#viewer@user:carol")
	deleteTuple(t, ds, storeID, "document:1", "viewer", "user:anne")

	replayed, err := runner.Replay(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, replayed.Batch.Status)
	require.Equal(t, first.Batch.Boundary, replayed.Batch.Boundary)
	require.Equal(t, first.Snapshot.Fingerprint, replayed.Snapshot.Fingerprint)
	require.True(t, resultByUser(replayed, "user:anne").Got)
	require.False(t, resultByUser(replayed, "user:carol").Got)

	// Continuing an already completed batch is idempotent and yields one report.
	again, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, first.Snapshot.Fingerprint, again.Snapshot.Fingerprint)

	diff, err := runner.CompareReports(context.Background(), storeID, batch.ID, batch.ID)
	require.NoError(t, err)
	require.False(t, diff.HasChanges())
}

// Two completed batches can be diffed to isolate which assertions changed.
func TestCompareTwoCompletedBatches(t *testing.T) {
	ds, storeID := bootstrapStore(t)

	runner := newRunner(t, ds)

	// Before the fix: anne has no access, but the assertion expects it.
	before, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID:    storeID,
		Assertions: []*openfgav1.Assertion{assertion("document:1", "viewer", "user:anne", true)},
	})
	require.NoError(t, err)
	beforeReport, err := runner.Continue(context.Background(), storeID, before.ID)
	require.NoError(t, err)
	require.Equal(t, 1, beforeReport.Snapshot.Mismatched)

	// Apply the fix.
	writeTuples(t, ds, storeID, "document:1#viewer@user:anne")

	after, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID:    storeID,
		Assertions: []*openfgav1.Assertion{assertion("document:1", "viewer", "user:anne", true)},
	})
	require.NoError(t, err)
	afterReport, err := runner.Continue(context.Background(), storeID, after.ID)
	require.NoError(t, err)
	require.Equal(t, 1, afterReport.Snapshot.Matched)

	diff, err := runner.CompareReports(context.Background(), storeID, before.ID, after.ID)
	require.NoError(t, err)
	require.True(t, diff.HasChanges())
	require.Equal(t, 1, diff.Changed)
	require.Equal(t, "document:1#viewer@user:anne", diff.Entries[0].Key)
	require.False(t, diff.Entries[0].A.Got)
	require.True(t, diff.Entries[0].B.Got)

	// Identical batches compare as unchanged.
	same, err := runner.CompareReports(context.Background(), storeID, after.ID, after.ID)
	require.NoError(t, err)
	require.False(t, same.HasChanges())
	require.Equal(t, 1, same.Same)

	// Comparing against an un-finished batch is rejected.
	pending, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID:    storeID,
		Assertions: []*openfgav1.Assertion{assertion("document:1", "viewer", "user:anne", true)},
	})
	require.NoError(t, err)
	_, err = runner.CompareReports(context.Background(), storeID, after.ID, pending.ID)
	require.ErrorIs(t, err, assertionrun.ErrBatchNotCompleted)
}

// Replaying a boundary past a full changelog page includes every change up to
// the pinned ULID (across full pages, the straddling page and the one-row tail)
// and excludes everything after it, while chunking the rebuilt tuples.
func TestBoundaryAcrossMultipleChangelogPages(t *testing.T) {
	ds, storeID := bootstrapStore(t)

	const before = 250 // > replayPageSize (200)
	const after = 10

	tuples := make([]string, 0, before+after)
	for i := 0; i < before; i++ {
		tuples = append(tuples, "document:1#viewer@user:u"+padIndex(i))
	}
	writeTuples(t, ds, storeID, tuples...)

	runner := newRunner(t, ds)

	batch, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID: storeID,
		Assertions: []*openfgav1.Assertion{
			assertion("document:1", "viewer", "user:u"+padIndex(0), true),        // first pre-boundary row
			assertion("document:1", "viewer", "user:u"+padIndex(before-1), true), // last pre-boundary row
			assertion("document:1", "viewer", "user:u"+padIndex(before), false),  // first post-boundary row
		},
	})
	require.NoError(t, err)

	post := make([]string, 0, after)
	for i := before; i < before+after; i++ {
		post = append(post, "document:1#viewer@user:u"+padIndex(i))
	}
	writeTuples(t, ds, storeID, post...)

	report, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, report.Batch.Status)

	require.True(t, resultByUser(report, "user:u"+padIndex(0)).Got)
	require.True(t, resultByUser(report, "user:u"+padIndex(before-1)).Got)
	require.False(t, resultByUser(report, "user:u"+padIndex(before)).Got)
	require.Equal(t, 3, report.Snapshot.Matched)
}

func padIndex(i int) string {
	return strings.Repeat("0", 3-len(strconv.Itoa(i))) + strconv.Itoa(i)
}

// A batch pinned against an empty changelog evaluates against no tuples.
func TestEmptyBoundarySeesNoTuples(t *testing.T) {
	ds, storeID := bootstrapStore(t)
	runner := newRunner(t, ds)

	batch, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID:    storeID,
		Assertions: []*openfgav1.Assertion{assertion("document:1", "viewer", "user:anne", false)},
	})
	require.NoError(t, err)
	require.Empty(t, batch.Boundary.ChangeULID)

	// A tuple written after pinning must remain invisible even with empty origin.
	writeTuples(t, ds, storeID, "document:1#viewer@user:anne")

	report, err := runner.Continue(context.Background(), storeID, batch.ID)
	require.NoError(t, err)
	require.Equal(t, assertionrun.ResultMatched, resultByUser(report, "user:anne").Status)
	require.False(t, resultByUser(report, "user:anne").Got)
}

func TestStartWithoutModelFails(t *testing.T) {
	ds := memory.New()
	t.Cleanup(ds.Close)
	storeID := ulid.Make().String() // store with no model

	runner := newRunner(t, ds)
	_, err := runner.Start(context.Background(), runsvc.StartRequest{
		StoreID:    storeID,
		Assertions: []*openfgav1.Assertion{assertion("document:1", "viewer", "user:anne", true)},
	})
	require.ErrorIs(t, err, runsvc.ErrAuthorizationModelNotFound)
}

func TestStartRejectsEmptyAssertions(t *testing.T) {
	ds, storeID := bootstrapStore(t)
	runner := newRunner(t, ds)

	_, err := runner.Start(context.Background(), runsvc.StartRequest{StoreID: storeID})
	require.ErrorIs(t, err, runsvc.ErrNoAssertions)
}
