package changeconsumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	// Start ahead of real time so changelog records are never "in the
	// future" relative to the clock.
	return &fakeClock{t: time.Now().UTC().Add(2 * time.Second)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestManager(t *testing.T, clock *fakeClock, ttl time.Duration) (*Manager, storage.OpenFGADatastore) {
	t.Helper()
	ds := memory.New()
	return New(ds.(Backend),
		WithClock(clock.now),
		WithLeaseTTL(ttl),
		WithMaxPageSize(100),
	), ds
}

func writeChange(t *testing.T, ds storage.OpenFGADatastore, store, objectType, user string) {
	t.Helper()
	obj := fmt.Sprintf("%s:%s", objectType, ulid.Make().String())
	tk := tuple.NewTupleKey(obj, "viewer", user)
	require.NoError(t, ds.Write(context.Background(), store, nil, []*openfgav1.TupleKey{tk}))
}

func drainPages(
	t *testing.T,
	ctx context.Context,
	mgr *Manager,
	lease *storage.ChangeLease,
	pageSize int,
) []*storage.PageReceipt {
	t.Helper()
	var receipts []*storage.PageReceipt
	for {
		page, err := mgr.ReadPage(ctx, lease, pageSize)
		require.NoError(t, err)
		if page.ChangeCount == 0 {
			return receipts
		}
		receipt, err := mgr.ConfirmPage(ctx, lease, page)
		require.NoError(t, err)
		receipts = append(receipts, receipt)
	}
}

// Only one worker may hold the lease for a named consumer at a time.
func TestAcquireIsExclusive(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, _ := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()

	leaseA, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), leaseA.Generation)

	_, err = mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-b",
	})
	require.ErrorIs(t, err, storage.ErrChangeLeaseHeld)

	// Renewal keeps the generation stable.
	renewed, err := mgr.RenewLease(ctx, leaseA)
	require.NoError(t, err)
	require.Equal(t, uint64(1), renewed.Generation)
	require.True(t, renewed.ExpiresAt.After(leaseA.ExpiresAt) || renewed.ExpiresAt.Equal(leaseA.ExpiresAt))

	// Validation.
	_, err = mgr.AcquireLease(ctx, AcquireLeaseRequest{StoreID: store, Name: "bad name!", HolderID: "a"})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.AcquireLease(ctx, AcquireLeaseRequest{StoreID: store, Name: "audit", HolderID: ""})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.ReadPage(ctx, leaseA, 0)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.ReadPage(ctx, leaseA, 101)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = mgr.ConfirmPage(ctx, leaseA, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// A worker that reads a page but dies before acknowledging it must not
// corrupt progress: the takeover worker re-reads the same page from durable
// progress, while the dead worker's generation is fenced.
func TestCrashBeforeAckReplaysOnTakeover(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()
	for i := 0; i < 4; i++ {
		writeChange(t, ds, store, "document", "user:jon")
	}

	leaseA, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)

	pageA, err := mgr.ReadPage(ctx, leaseA, 2)
	require.NoError(t, err)
	require.Equal(t, 2, pageA.ChangeCount)
	require.Empty(t, pageA.StartULID)
	require.Equal(t, uint64(1), pageA.Generation)

	// Worker A crashes before ConfirmPage. Lease TTL elapses.
	clock.advance(2 * time.Minute)

	// Worker B takes over: generation switch, but progress is still empty.
	leaseB, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-b",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), leaseB.Generation)

	state, err := mgr.GetState(ctx, store, "audit")
	require.NoError(t, err)
	require.Empty(t, state.ProgressULID)

	// B re-reads from the durable continuation point and gets the exact same
	// page A had, including an identical fingerprint.
	pageB, err := mgr.ReadPage(ctx, leaseB, 2)
	require.NoError(t, err)
	require.Equal(t, pageA.StartULID, pageB.StartULID)
	require.Equal(t, pageA.EndULID, pageB.EndULID)
	require.Equal(t, pageA.ChangeCount, pageB.ChangeCount)
	require.Equal(t, pageA.Checksum, pageB.Checksum)
	require.Equal(t, pageA.Changes(), pageB.Changes())

	// A waking up cannot confirm anything anymore.
	_, err = mgr.ConfirmPage(ctx, leaseA, pageA)
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)
	_, err = mgr.ReadPage(ctx, leaseA, 2)
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)
	err = mgr.Release(ctx, leaseA)
	require.ErrorIs(t, err, storage.ErrChangeLeaseFenced)

	// B acknowledges. Durable progress and receipt now exist.
	receipt1, err := mgr.ConfirmPage(ctx, leaseB, pageB)
	require.NoError(t, err)
	require.Equal(t, uint64(1), receipt1.PageNumber)
	require.Equal(t, uint64(2), receipt1.Generation)
	require.Equal(t, pageB.EndULID, receipt1.EndULID)

	rest, err := mgr.ReadPage(ctx, leaseB, 10)
	require.NoError(t, err)
	require.Equal(t, 2, rest.ChangeCount)
	require.Equal(t, pageB.EndULID, rest.StartULID)
	receipt2, err := mgr.ConfirmPage(ctx, leaseB, rest)
	require.NoError(t, err)
	require.Equal(t, uint64(2), receipt2.PageNumber)
	require.Equal(t, receipt1.EndULID, receipt2.StartULID)

	// Both receipts are independently verifiable via legacy pagination.
	for _, n := range []uint64{1, 2} {
		r, err := mgr.VerifyReceipt(ctx, store, "audit", n)
		require.NoError(t, err)
		require.Equal(t, n, r.PageNumber)
	}
}

// Service restart against the same durable backend: the holder renews and
// resumes exactly at the persisted progress; receipt history remains
// queryable.
func TestRestartResumesFromDurableProgress(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	ds := memory.New()
	mgr1 := New(ds.(Backend), WithClock(clock.now), WithLeaseTTL(time.Minute))
	store := ulid.Make().String()
	for i := 0; i < 5; i++ {
		writeChange(t, ds, store, "document", "user:jon")
	}

	lease, err := mgr1.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)
	receipts := drainPages(t, ctx, mgr1, lease, 2)
	require.Len(t, receipts, 3) // 2, 2, 1

	// Restart: brand new manager instance (as a rebuilt audit service would
	// have), same backend, same holder, lease still live.
	clock.advance(10 * time.Second)
	mgr2 := New(ds.(Backend), WithClock(clock.now), WithLeaseTTL(time.Minute))
	lease2, err := mgr2.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), lease2.Generation, "renewal within TTL must not switch generation")

	state, err := mgr2.GetState(ctx, store, "audit")
	require.NoError(t, err)
	require.Equal(t, receipts[2].EndULID, state.ProgressULID)
	require.Equal(t, uint64(3), state.LastReceipt.PageNumber)

	page, err := mgr2.ReadPage(ctx, lease2, 10)
	require.NoError(t, err)
	require.Zero(t, page.ChangeCount, "already fully consumed")
	require.Equal(t, state.ProgressULID, page.StartULID)

	// Receipt query across the restart gives identical data.
	r2, err := mgr2.GetReceipt(ctx, store, "audit", 2)
	require.NoError(t, err)
	require.Equal(t, receipts[1], r2)
	_, err = mgr2.VerifyReceipt(ctx, store, "audit", 2)
	require.NoError(t, err)

	// New changes arriving after caught-up continue the same chain.
	writeChange(t, ds, store, "document", "user:new")
	page, err = mgr2.ReadPage(ctx, lease2, 10)
	require.NoError(t, err)
	require.Equal(t, 1, page.ChangeCount)
	require.Equal(t, state.ProgressULID, page.StartULID)
	receipt4, err := mgr2.ConfirmPage(ctx, lease2, page)
	require.NoError(t, err)
	require.Equal(t, uint64(4), receipt4.PageNumber)
	require.Equal(t, state.ProgressULID, receipt4.StartULID)
}

// End-to-end: delivery pages, receipt verification through the legacy
// ReadChanges interface, and agreement at the head of the stream.
func TestEndToEndReceiptsAgreeWithLegacyPagination(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()
	for i := 0; i < 7; i++ {
		writeChange(t, ds, store, "document", "user:jon")
	}

	lease, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "billing-audit", HolderID: "worker-a",
	})
	require.NoError(t, err)
	receipts := drainPages(t, ctx, mgr, lease, 3)
	require.Len(t, receipts, 3) // 3, 3, 1

	// Contiguous boundaries.
	require.Empty(t, receipts[0].StartULID)
	for i := 1; i < len(receipts); i++ {
		require.Equal(t, receipts[i-1].EndULID, receipts[i].StartULID)
	}

	// Every receipt verifies against the legacy paging interface.
	for i, r := range receipts {
		got, err := mgr.VerifyReceipt(ctx, store, "billing-audit", uint64(i+1))
		require.NoError(t, err)
		require.Equal(t, r, got)
	}

	// Receipt query for a missing page is ErrNotFound.
	_, err = mgr.GetReceipt(ctx, store, "billing-audit", 4)
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = mgr.VerifyReceipt(ctx, store, "billing-audit", 4)
	require.ErrorIs(t, err, storage.ErrNotFound)

	// Unknown consumer.
	_, err = mgr.GetState(ctx, store, "nobody")
	require.ErrorIs(t, err, storage.ErrChangeConsumerNotFound)

	// Legacy ReadChanges past durable progress reports nothing further.
	state, err := mgr.GetState(ctx, store, "billing-audit")
	require.NoError(t, err)
	_, _, err = ds.ReadChanges(ctx, store, storage.ReadChangesFilter{},
		storage.ReadChangesOptions{Pagination: storage.NewPaginationOptions(10, state.ProgressULID)})
	require.ErrorIs(t, err, storage.ErrNotFound)
}

// A type-scoped consumer only delivers matching changes, and receipts still
// verify through the type-filtered legacy pagination path.
func TestScopedConsumer(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()
	for i := 0; i < 3; i++ {
		writeChange(t, ds, store, "document", "user:jon")
		writeChange(t, ds, store, "group", "user:jon")
	}

	lease, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "doc-only", HolderID: "worker-a", ObjectType: "group",
	})
	require.NoError(t, err)

	page, err := mgr.ReadPage(ctx, lease, 10)
	require.NoError(t, err)
	require.Equal(t, 3, page.ChangeCount)
	for _, e := range page.Entries {
		objType, _ := tuple.SplitObject(e.Change.GetTupleKey().GetObject())
		require.Equal(t, "group", objType)
	}

	receipt, err := mgr.ConfirmPage(ctx, lease, page)
	require.NoError(t, err)
	require.Equal(t, "group", receipt.ObjectType)
	verified, err := mgr.VerifyReceipt(ctx, store, "doc-only", 1)
	require.NoError(t, err)
	require.Equal(t, receipt, verified)

	// Scope cannot change later.
	_, err = mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "doc-only", HolderID: "worker-a", ObjectType: "document",
	})
	require.ErrorIs(t, err, storage.ErrChangeConsumerScopeMismatch)
}

// Explicit release allows immediate hand-off without waiting for the TTL.
func TestReleaseHandsOffImmediately(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()
	writeChange(t, ds, store, "document", "user:jon")

	leaseA, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(ctx, leaseA))

	// Despite a fresh lease and no elapsed TTL, B gets in via a generation
	// switch.
	clock.advance(time.Second)
	leaseB, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-b",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), leaseB.Generation)

	page, err := mgr.ReadPage(ctx, leaseB, 10)
	require.NoError(t, err)
	require.Equal(t, 1, page.ChangeCount)
}

// A heartbeat keeps the generation stable across many read/confirm cycles.
func TestHeartbeatKeepsGenerationStable(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()

	lease, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
		StoreID: store, Name: "audit", HolderID: "worker-a",
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		writeChange(t, ds, store, "document", "user:jon")
		clock.advance(15 * time.Second)
		lease, err = mgr.RenewLease(ctx, lease)
		require.NoError(t, err)
		require.Equal(t, uint64(1), lease.Generation)
		page, err := mgr.ReadPage(ctx, lease, 10)
		require.NoError(t, err)
		require.Equal(t, 1, page.ChangeCount)
		_, err = mgr.ConfirmPage(ctx, lease, page)
		require.NoError(t, err)
	}

	state, err := mgr.GetState(ctx, store, "audit")
	require.NoError(t, err)
	require.Equal(t, uint64(5), state.LastReceipt.PageNumber)
	require.Equal(t, uint64(1), state.Generation)
}

// Concurrent contenders must end with exactly one live holder and an intact
// receipt chain.
func TestConcurrentContendersAndConfirmers(t *testing.T) {
	ctx := context.Background()
	clock := newFakeClock()
	mgr, ds := newTestManager(t, clock, time.Minute)
	store := ulid.Make().String()
	for i := 0; i < 4; i++ {
		writeChange(t, ds, store, "document", "user:jon")
	}

	const n = 16
	var wg sync.WaitGroup
	holders := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			holder := fmt.Sprintf("worker-%02d", i)
			lease, err := mgr.AcquireLease(ctx, AcquireLeaseRequest{
				StoreID: store, Name: "audit", HolderID: holder,
			})
			if err == nil {
				holders <- holder
				// Winner opportunistically drains everything.
				_ = drainPages(t, ctx, mgr, lease, 2)
				return
			}
			require.True(t, errors.Is(err, storage.ErrChangeLeaseHeld), "unexpected err %v", err)
		}(i)
	}
	wg.Wait()
	close(holders)

	var winners []string
	for h := range holders {
		winners = append(winners, h)
	}
	require.Len(t, winners, 1)

	state, err := mgr.GetState(ctx, store, "audit")
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.Generation)
	require.NotNil(t, state.LastReceipt)
	require.Equal(t, state.ProgressULID, state.LastReceipt.EndULID)
	for n := uint64(1); n <= state.LastReceipt.PageNumber; n++ {
		_, err := mgr.VerifyReceipt(ctx, store, "audit", n)
		require.NoError(t, err)
	}
}
