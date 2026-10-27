package test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

// changeConsumerDatastore is implemented by backends that support named
// change consumers in addition to the standard datastore contract.
type changeConsumerDatastore interface {
	storage.OpenFGADatastore
	storage.ChangeConsumerBackend
}

// RunChangeConsumerTests exercises the generation-fenced lease, durable
// progress and complete-page receipt contract against any backend.
func RunChangeConsumerTests(t *testing.T, ds changeConsumerDatastore) {
	t.Run("TestLeaseAcquireRenewAndTakeover", func(t *testing.T) { leaseLifecycleTest(t, ds) })
	t.Run("TestPageChainingReceiptsAndFencing", func(t *testing.T) { pageChainingTest(t, ds) })
	t.Run("TestReceiptsMatchLegacyPagination", func(t *testing.T) { receiptLegacyPaginationTest(t, ds) })
	t.Run("TestLeaseRelease", func(t *testing.T) { leaseReleaseTest(t, ds) })
	t.Run("TestConcurrentAcquire", func(t *testing.T) { concurrentAcquireTest(t, ds) })
	t.Run("TestConcurrentConfirmIsIdempotent", func(t *testing.T) { concurrentConfirmTest(t, ds) })
	t.Run("TestScopeIsPinned", func(t *testing.T) { scopePinningTest(t, ds) })
}

func writeChangeTuples(t *testing.T, ds storage.OpenFGADatastore, store string, n int, user string) {
	t.Helper()
	for i := 0; i < n; i++ {
		tk := tuple.NewTupleKey("document:"+ulid.Make().String(), "viewer", user)
		require.NoError(t, ds.Write(context.Background(), store, nil, []*openfgav1.TupleKey{tk}))
	}
}

func leaseLifecycleTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	writeChangeTuples(t, ds, store, 5, "user:jon")

	const consumer, holderA, holderB = "audit", "worker-a", "worker-b"
	t0 := time.Unix(1_700_000_000, 0).UTC()

	initial, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderA, TTL: time.Minute, Now: t0,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), initial.Generation)
	require.Equal(t, holderA, initial.Holder)
	require.Empty(t, initial.ProgressULID)
	require.Nil(t, initial.LastReceipt)
	require.Equal(t, t0.Add(time.Minute), initial.ExpiresAt)

	// Same holder renews: generation must stay stable, deadline extended.
	renewed, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderA, TTL: time.Minute, Now: t0.Add(30 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), renewed.Generation)
	require.Equal(t, t0.Add(90*time.Second), renewed.ExpiresAt)

	// Another holder cannot steal a live lease.
	_, err = ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderB, TTL: time.Minute, Now: t0.Add(31 * time.Second),
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseHeld)

	// Holder A drains and confirms everything.
	page, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1, PageSize: 100, Now: t0.Add(32 * time.Second),
	})
	require.NoError(t, err)
	require.Len(t, page.Entries, 5)
	require.Equal(t, uint64(1), page.Generation)
	require.Empty(t, page.StartULID)

	receipt, err := ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1,
		EndULID: page.EndULID, ChangeCount: page.ChangeCount, Checksum: page.Checksum, Now: t0.Add(33 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), receipt.PageNumber)

	// Lease expires; B takes over. Progress and receipts survive the switch.
	takenOver, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderB, TTL: time.Minute, Now: t0.Add(200 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), takenOver.Generation)
	require.Equal(t, holderB, takenOver.Holder)
	require.Equal(t, page.EndULID, takenOver.ProgressULID)
	require.NotNil(t, takenOver.LastReceipt)
	require.Equal(t, page.EndULID, takenOver.LastReceipt.EndULID)

	// B resumes exactly at durable progress: nothing to re-deliver.
	emptyPage, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderB, Generation: 2, PageSize: 100, Now: t0.Add(201 * time.Second),
	})
	require.NoError(t, err)
	require.Empty(t, emptyPage.Entries)
	require.Equal(t, page.EndULID, emptyPage.StartULID)
	require.Equal(t, page.EndULID, emptyPage.EndULID)

	// A is now fenced on every mutating operation.
	_, err = ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1, PageSize: 10, Now: t0.Add(202 * time.Second),
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)
	_, err = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1,
		StartULID: page.EndULID, EndULID: ulid.Make().String(), ChangeCount: 1, Checksum: "x", Now: t0.Add(202 * time.Second),
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)

	// Same holder returning after its own expiry is also a generation switch.
	takenOver3, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderB, TTL: time.Minute, Now: t0.Add(300 * time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), takenOver3.Generation)
}

func pageChainingTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	writeChangeTuples(t, ds, store, 10, "user:jon")

	const consumer, holder = "auditor", "worker-a"
	now := time.Unix(1_700_000_000, 0).UTC()

	_, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)

	// Unknown consumer is a distinct error from fencing.
	_, err = ds.GetChangeConsumerState(ctx, store, "missing")
	require.ErrorIs(t, err, storage.ErrChangeConsumerNotFound)
	_, err = ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: "missing", Holder: holder, Generation: 1, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeConsumerNotFound)
	_, err = ds.GetChangePageReceipt(ctx, store, "missing", 1)
	require.ErrorIs(t, err, storage.ErrChangeConsumerNotFound)

	// Drain in pages of 3: receipts must form a gap-free chain. Ten changes
	// are delivered as 3, 3, 3, 1.
	expectedCounts := []int{3, 3, 3, 1}
	var prevEnd string
	for pageNum := uint64(1); pageNum <= uint64(len(expectedCounts)); pageNum++ {
		page, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
			Store: store, Consumer: consumer, Holder: holder, Generation: 1, PageSize: 3, Now: now,
		})
		require.NoError(t, err)
		require.Equal(t, expectedCounts[pageNum-1], page.ChangeCount)
		require.Equal(t, prevEnd, page.StartULID)
		require.NotEmpty(t, page.EndULID)

		receipt, err := ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
			Store: store, Consumer: consumer, Holder: holder, Generation: 1,
			StartULID: page.StartULID, EndULID: page.EndULID,
			ChangeCount: page.ChangeCount, Checksum: page.Checksum, Now: now,
		})
		require.NoError(t, err)
		require.Equal(t, pageNum, receipt.PageNumber)
		require.Equal(t, page.StartULID, receipt.StartULID)
		require.Equal(t, page.EndULID, receipt.EndULID)
		require.Equal(t, page.ChangeCount, receipt.ChangeCount)
		require.Equal(t, page.Checksum, receipt.Checksum)

		stored, err := ds.GetChangePageReceipt(ctx, store, consumer, pageNum)
		require.NoError(t, err)
		require.Equal(t, receipt, stored)

		state, err := ds.GetChangeConsumerState(ctx, store, consumer)
		require.NoError(t, err)
		require.Equal(t, page.EndULID, state.ProgressULID)
		require.Equal(t, receipt.PageNumber, state.LastReceipt.PageNumber)

		prevEnd = page.EndULID
	}

	// Caught up: further reads are empty and cannot be receipted.
	emptyPage, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holder, Generation: 1, PageSize: 3, Now: now,
	})
	require.NoError(t, err)
	require.Zero(t, emptyPage.ChangeCount)
	require.Equal(t, prevEnd, emptyPage.StartULID)
	require.Equal(t, prevEnd, emptyPage.EndULID)

	_, err = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: store, Consumer: consumer, Holder: holder, Generation: 1,
		StartULID: prevEnd, EndULID: prevEnd, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrInvalidPageReceipt)

	// Out-of-range receipt numbers.
	_, err = ds.GetChangePageReceipt(ctx, store, consumer, 0)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = ds.GetChangePageReceipt(ctx, store, consumer, 5)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// Idempotent re-confirmation of the tip returns the same receipt.
	last, err := ds.GetChangePageReceipt(ctx, store, consumer, 4)
	require.NoError(t, err)
	again, err := ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: store, Consumer: consumer, Holder: holder, Generation: 1,
		StartULID: last.StartULID, EndULID: last.EndULID,
		ChangeCount: last.ChangeCount, Checksum: last.Checksum, Now: now,
	})
	require.NoError(t, err)
	require.Equal(t, last, again)
	state, err := ds.GetChangeConsumerState(ctx, store, consumer)
	require.NoError(t, err)
	require.Equal(t, uint64(4), state.LastReceipt.PageNumber)

	// A confirm that skips the persisted boundary is rejected.
	_, err = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: store, Consumer: consumer, Holder: holder, Generation: 1,
		StartULID: "", EndULID: prevEnd, ChangeCount: 10, Checksum: "x", Now: now,
	})
	require.ErrorIs(t, err, storage.ErrInvalidPageReceipt)

	// Tampered fingerprint / count are rejected. Tamper proof: forge a page
	// that chains correctly but disagrees with the changelog.
	forgeStore := ulid.Make().String()
	writeChangeTuples(t, ds, forgeStore, 2, "user:jon")
	_, err = ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: forgeStore, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)
	forgePage, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: forgeStore, Consumer: consumer, Holder: holder, Generation: 1, PageSize: 10, Now: now,
	})
	require.NoError(t, err)
	_, err = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: forgeStore, Consumer: consumer, Holder: holder, Generation: 1,
		StartULID: forgePage.StartULID, EndULID: forgePage.EndULID,
		ChangeCount: forgePage.ChangeCount, Checksum: "deadbeef", Now: now,
	})
	require.ErrorIs(t, err, storage.ErrPageChecksumMismatch)
	_, err = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
		Store: forgeStore, Consumer: consumer, Holder: holder, Generation: 1,
		StartULID: forgePage.StartULID, EndULID: forgePage.EndULID,
		ChangeCount: forgePage.ChangeCount + 1, Checksum: forgePage.Checksum, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrPageChecksumMismatch)
	// Failed confirms must not move progress.
	forgeState, err := ds.GetChangeConsumerState(ctx, forgeStore, consumer)
	require.NoError(t, err)
	require.Empty(t, forgeState.ProgressULID)
	require.Nil(t, forgeState.LastReceipt)
}

func receiptLegacyPaginationTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	writeChangeTuples(t, ds, store, 6, "user:legacy")

	const consumer, holder = "legacy-auditor", "worker-a"
	now := time.Unix(1_700_000_000, 0).UTC()

	_, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)

	// Consume pages of 2.
	var receipts []*storage.PageReceipt
	for {
		page, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
			Store: store, Consumer: consumer, Holder: holder, Generation: 1, PageSize: 2, Now: now,
		})
		require.NoError(t, err)
		if page.ChangeCount == 0 {
			break
		}
		receipt, err := ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
			Store: store, Consumer: consumer, Holder: holder, Generation: 1,
			StartULID: page.StartULID, EndULID: page.EndULID,
			ChangeCount: page.ChangeCount, Checksum: page.Checksum, Now: now,
		})
		require.NoError(t, err)
		receipts = append(receipts, receipt)
	}
	require.Len(t, receipts, 3)

	// Every receipt must be independently reproducible via the legacy
	// ReadChanges pagination interface (page size 1 makes boundaries exact).
	for _, receipt := range receipts {
		var replayed []*openfgav1.TupleChange
		token := receipt.StartULID
		for {
			changes, cont, err := ds.ReadChanges(ctx, store, storage.ReadChangesFilter{ObjectType: receipt.ObjectType},
				storage.ReadChangesOptions{Pagination: storage.NewPaginationOptions(1, token)})
			if errors.Is(err, storage.ErrNotFound) {
				break
			}
			require.NoError(t, err)
			require.Len(t, changes, 1)
			replayed = append(replayed, changes[0])
			token = cont
			if cont == receipt.EndULID {
				break
			}
		}
		require.Len(t, replayed, receipt.ChangeCount)
		require.Equal(t, receipt.Checksum, storage.ChangePageChecksum(replayed))
	}

	// Legacy paging past the durable progress reports no further changes.
	state, err := ds.GetChangeConsumerState(ctx, store, consumer)
	require.NoError(t, err)
	_, _, err = ds.ReadChanges(ctx, store, storage.ReadChangesFilter{},
		storage.ReadChangesOptions{Pagination: storage.NewPaginationOptions(10, state.ProgressULID)})
	require.ErrorIs(t, err, storage.ErrNotFound)

	// A takeover holder re-reading from durable progress obtains a page whose
	// contents match the next legacy page beyond the last receipt.
	taken, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: "worker-b", TTL: time.Minute, Now: now.Add(2 * time.Minute),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), taken.Generation)
	page, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: "worker-b", Generation: 2, PageSize: 10, Now: now.Add(2 * time.Minute),
	})
	require.NoError(t, err)
	require.Zero(t, page.ChangeCount)
	require.Equal(t, state.ProgressULID, page.StartULID)
}

func leaseReleaseTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	now := time.Unix(1_700_000_000, 0).UTC()

	const consumer, holderA, holderB = "releaser", "worker-a", "worker-b"
	_, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderA, TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)

	// Wrong generation cannot release.
	err = ds.ReleaseChangeLease(ctx, storage.ReleaseChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 99, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)

	require.NoError(t, ds.ReleaseChangeLease(ctx, storage.ReleaseChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1, Now: now,
	}))

	// The released holder is fenced even though the TTL has not elapsed.
	_, err = ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holderA, Generation: 1, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)

	// Immediate hand-off is a generation switch without waiting for TTL.
	taken, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holderB, TTL: time.Minute, Now: now.Add(time.Second),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), taken.Generation)
	require.Equal(t, holderB, taken.Holder)

	// Releasing an unknown consumer is not found.
	err = ds.ReleaseChangeLease(ctx, storage.ReleaseChangeLeaseOptions{
		Store: store, Consumer: "missing", Holder: holderB, Generation: 1, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeConsumerNotFound)
}

func concurrentAcquireTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	now := time.Unix(1_700_000_000, 0).UTC()

	const consumer = "race"
	const contenders = 20

	round := func(at time.Time, winners *int, gen uint64) {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for i := 0; i < contenders; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				holder := "worker-" + ulid.Make().String()
				state, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
					Store: store, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: at,
				})
				if err == nil {
					require.Equal(t, gen, state.Generation)
					mu.Lock()
					*winners++
					mu.Unlock()
				} else {
					require.ErrorIs(t, err, storage.ErrChangeLeaseHeld)
				}
			}(i)
		}
		wg.Wait()
	}

	var winners int
	round(now, &winners, 1)
	require.Equal(t, 1, winners)

	// Still inside the TTL: nobody else can acquire.
	round(now.Add(30*time.Second), &winners, 1)
	require.Equal(t, 1, winners)

	// After expiry, exactly one contender performs the generation switch.
	round(now.Add(2*time.Minute), &winners, 2)
	require.Equal(t, 2, winners)

	state, err := ds.GetChangeConsumerState(ctx, store, consumer)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.Generation)
}

func concurrentConfirmTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	writeChangeTuples(t, ds, store, 3, "user:race")

	const consumer, holder = "race-confirm", "worker-a"
	now := time.Unix(1_700_000_000, 0).UTC()
	_, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)

	page, err := ds.ReadChangePage(ctx, storage.ReadChangePageOptions{
		Store: store, Consumer: consumer, Holder: holder, Generation: 1, PageSize: 10, Now: now,
	})
	require.NoError(t, err)
	require.Equal(t, 3, page.ChangeCount)

	const waiters = 16
	var wg sync.WaitGroup
	errs := make([]error, waiters)
	for i := 0; i < waiters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = ds.ConfirmChangePage(ctx, storage.ConfirmChangePageOptions{
				Store: store, Consumer: consumer, Holder: holder, Generation: 1,
				StartULID: page.StartULID, EndULID: page.EndULID,
				ChangeCount: page.ChangeCount, Checksum: page.Checksum, Now: now,
			})
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}

	state, err := ds.GetChangeConsumerState(ctx, store, consumer)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.LastReceipt.PageNumber)
	require.Equal(t, page.EndULID, state.ProgressULID)
}

func scopePinningTest(t *testing.T, ds changeConsumerDatastore) {
	ctx := context.Background()
	store := ulid.Make().String()
	now := time.Unix(1_700_000_000, 0).UTC()

	const consumer, holder = "scoped", "worker-a"
	created, err := ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, ObjectType: "document", TTL: time.Minute, Now: now,
	})
	require.NoError(t, err)
	require.Equal(t, "document", created.ObjectType)

	_, err = ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, ObjectType: "group", TTL: time.Minute, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeConsumerScopeMismatch)

	// Dropping the scope on renewal is also a mismatch.
	_, err = ds.AcquireChangeLease(ctx, storage.AcquireChangeLeaseOptions{
		Store: store, Consumer: consumer, Holder: holder, TTL: time.Minute, Now: now,
	})
	require.ErrorIs(t, err, storage.ErrChangeConsumerScopeMismatch)
}
