package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/assertionrun"
	ledgermem "github.com/openfga/openfga/pkg/storage/assertionrun/memory"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

func ctx() context.Context { return context.Background() }

func sampleBatch(store, id string, total int) *assertionrun.Batch {
	now := time.Now().UTC()
	return &assertionrun.Batch{
		ID:              id,
		StoreID:         store,
		Boundary:        assertionrun.Boundary{StoreID: store, AuthorizationModelID: "01HZ", ChangeULID: "01HX", CapturedAt: now},
		Status:          assertionrun.StatusRunning,
		TotalAssertions: total,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

func sampleAssertions() []*openfgav1.Assertion {
	return []*openfgav1.Assertion{
		{TupleKey: tupleUtils.NewAssertionTupleKey("doc:1", "viewer", "user:a"), Expectation: true},
		{TupleKey: tupleUtils.NewAssertionTupleKey("doc:1", "viewer", "user:b"), Expectation: false},
	}
}

func TestInitAndGetBatch(t *testing.T) {
	ledger := ledgermem.New()

	batch, err := ledger.InitBatch(ctx(), sampleBatch("store", "batch", 2), sampleAssertions())
	require.NoError(t, err)
	require.Equal(t, "batch", batch.ID)

	got, err := ledger.GetBatch(ctx(), "store", "batch")
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusRunning, got.Status)
	require.Equal(t, 2, got.TotalAssertions)

	_, err = ledger.GetBatch(ctx(), "store", "missing")
	require.ErrorIs(t, err, assertionrun.ErrBatchNotFound)
}

func TestInitBatchRejectsDuplicate(t *testing.T) {
	ledger := ledgermem.New()

	_, err := ledger.InitBatch(ctx(), sampleBatch("store", "batch", 2), sampleAssertions())
	require.NoError(t, err)

	_, err = ledger.InitBatch(ctx(), sampleBatch("store", "batch", 2), sampleAssertions())
	require.ErrorIs(t, err, assertionrun.ErrBatchAlreadyExists)
}

func TestPinnedAssertionsAreImmutableCopies(t *testing.T) {
	ledger := ledgermem.New()

	in := sampleAssertions()
	_, err := ledger.InitBatch(ctx(), sampleBatch("store", "batch", 2), in)
	require.NoError(t, err)

	// Mutate the caller's slice and its proto after initialization.
	in[0].Expectation = false
	in[0].TupleKey.User = "user:tampered"

	got, err := ledger.GetAssertions(ctx(), "store", "batch")
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.True(t, got[0].GetExpectation())
	require.Equal(t, "user:a", got[0].GetTupleKey().GetUser())
}

func TestPutAndGetResultsUpsertOrdered(t *testing.T) {
	ledger := ledgermem.New()
	_, err := ledger.InitBatch(ctx(), sampleBatch("store", "batch", 2), sampleAssertions())
	require.NoError(t, err)

	write := func(idx int, status assertionrun.ResultStatus, attempts int) {
		require.NoError(t, ledger.PutResult(ctx(), "store", "batch", &assertionrun.Result{
			BatchID:   "batch",
			Index:     idx,
			Assertion: sampleAssertions()[idx],
			Expected:  true,
			Got:       status == assertionrun.ResultMatched,
			Status:    status,
			Attempts:  attempts,
			UpdatedAt: time.Now().UTC(),
		}))
	}

	write(1, assertionrun.ResultError, 1)
	write(0, assertionrun.ResultMatched, 1)
	// Upsert index 1.
	write(1, assertionrun.ResultMatched, 2)

	results, err := ledger.GetResults(ctx(), "store", "batch")
	require.NoError(t, err)
	require.Len(t, results, 2)
	require.Equal(t, 0, results[0].Index)
	require.Equal(t, 1, results[1].Index)
	require.Equal(t, assertionrun.ResultMatched, results[1].Status)
	require.Equal(t, 2, results[1].Attempts)

	_, err = ledger.GetResults(ctx(), "store", "missing")
	require.ErrorIs(t, err, assertionrun.ErrBatchNotFound)
}

func TestPutResultUnknownBatchErrors(t *testing.T) {
	ledger := ledgermem.New()
	err := ledger.PutResult(ctx(), "store", "batch", &assertionrun.Result{Index: 0})
	require.ErrorIs(t, err, assertionrun.ErrBatchNotFound)
}

func TestUpdateBatch(t *testing.T) {
	ledger := ledgermem.New()
	_, err := ledger.InitBatch(ctx(), sampleBatch("store", "batch", 1), sampleAssertions()[:1])
	require.NoError(t, err)

	updated, err := ledger.UpdateBatch(ctx(), "store", "batch", func(b *assertionrun.Batch) error {
		b.Status = assertionrun.StatusCompleted
		b.CompletedAt = time.Now().UTC()
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, updated.Status)
	require.False(t, updated.CompletedAt.IsZero())

	boom := errors.New("boom")
	_, err = ledger.UpdateBatch(ctx(), "store", "batch", func(*assertionrun.Batch) error { return boom })
	require.ErrorIs(t, err, boom)

	// A failed mutation must not be persisted.
	got, err := ledger.GetBatch(ctx(), "store", "batch")
	require.NoError(t, err)
	require.Equal(t, assertionrun.StatusCompleted, got.Status)

	_, err = ledger.UpdateBatch(ctx(), "store", "missing", func(*assertionrun.Batch) error { return nil })
	require.ErrorIs(t, err, assertionrun.ErrBatchNotFound)
}

func TestListBatchesNewestFirst(t *testing.T) {
	ledger := ledgermem.New()

	older := sampleBatch("store", "older", 1)
	older.CreatedAt = time.Now().Add(-time.Hour).UTC()
	newer := sampleBatch("store", "newer", 1)
	newer.CreatedAt = time.Now().UTC()

	_, err := ledger.InitBatch(ctx(), older, sampleAssertions()[:1])
	require.NoError(t, err)
	_, err = ledger.InitBatch(ctx(), newer, sampleAssertions()[:1])
	require.NoError(t, err)

	batches, err := ledger.ListBatches(ctx(), "store")
	require.NoError(t, err)
	require.Len(t, batches, 2)
	require.Equal(t, "newer", batches[0].ID)
	require.Equal(t, "older", batches[1].ID)
}
