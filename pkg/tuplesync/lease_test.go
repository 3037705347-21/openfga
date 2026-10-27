package tuplesync

import (
	crand "crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

// testEntropy is independent from ulid's global monotonic pool: seeding
// back-dated ULIDs through the global pool would corrupt ordering of later
// ulid.Make() calls within the same millisecond.
var testEntropy = ulid.Monotonic(crand.Reader, 0)

func seedLeaseHolder(t *testing.T, h *testHarness, controlID, targetID, source string, at time.Time) string {
	t.Helper()
	id, err := ulid.New(ulid.Timestamp(at), testEntropy)
	require.NoError(t, err)
	obj := leaseAttemptObject(targetID, source, id.String())
	require.NoError(t, h.ds.Write(h.ctx, controlID, nil, []*openfgav1.TupleKey{
		{Object: obj, Relation: relHolder, User: leaseActiveUser},
	}))
	return obj
}

func TestLeaseObjectEnvelope(t *testing.T) {
	obj := leaseAttemptObject("01HSTORE00000000000000000A", stringsRepeated("s", 200), "01H2K30000000000000000000")
	typ, id := splitObjectForTest(obj)
	require.LessOrEqual(t, len(typ), mysqlMaxObjectTypeLength)
	require.LessOrEqual(t, len(id), mysqlMaxObjectIDLength)
	require.LessOrEqual(t, len(leaseActiveUser), mysqlMaxUserLength)
	require.LessOrEqual(t, len(relHolder), mysqlMaxRelationLength)
}

func TestAcquireLeaseConcurrentSingleWinner(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()

	const n = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	results := make(chan error, n)
	acquired := make(chan *Lease, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			l, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
			if err == nil {
				results <- nil
				acquired <- l // hold until every contender has finished
				return
			}
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(acquired)

	var wins, inProgress int
	for err := range results {
		switch {
		case err == nil:
			wins++
		case errors.Is(err, ErrSyncInProgress):
			inProgress++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	require.Equal(t, 1, wins, "exactly one acquirer must win")
	require.Equal(t, n-1, inProgress)

	for l := range acquired {
		require.NoError(t, l.Release(h.ctx))
	}

	// every withdrawn attempt cleaned itself up; nothing remains after the
	// winner released.
	attempts, err := readAttempts(h.ctx, h.backend, controlID, leaseKeyPrefix(h.targetID, "dir"))
	require.NoError(t, err)
	require.Empty(t, attempts)
}

func TestAcquireLeaseRejectsFreshHolder(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()
	obj := seedLeaseHolder(t, h, controlID, h.targetID, "dir", time.Now())

	_, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
	require.ErrorIs(t, err, ErrSyncInProgress)

	// the existing holder must remain untouched
	exists := h.tupleExists(controlID, obj, relHolder, leaseActiveUser)
	require.True(t, exists)
}

func TestAcquireLeaseTakesOverStaleHolder(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()
	staleObj := seedLeaseHolder(t, h, controlID, h.targetID, "dir", time.Now().Add(-2*time.Minute))

	l, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
	require.NoError(t, err)
	require.NotEqual(t, staleObj, l.object)

	// stale holder removed, exactly the new attempt remains
	require.False(t, h.tupleExists(controlID, staleObj, relHolder, leaseActiveUser),
		"stale crashed holder must be cleaned up")
	require.True(t, h.tupleExists(controlID, l.object, relHolder, leaseActiveUser))

	require.NoError(t, l.Release(h.ctx))
}

func TestLeaseReleaseDeletesOnlyOwnAttempt(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()

	l1, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
	require.NoError(t, err)

	// a stale foreign attempt object; releasing some OTHER object must not
	// touch l1.
	staleObj := seedLeaseHolder(t, h, controlID, h.targetID, "dir", time.Now().Add(-time.Hour))
	other := &Lease{b: h.backend, controlID: controlID, object: staleObj}
	require.NoError(t, other.Release(h.ctx))
	_, err = acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
	require.ErrorIs(t, err, ErrSyncInProgress, "original holder must still own the lease")

	require.NoError(t, l1.Release(h.ctx))
	l2, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "dir", time.Minute, time.Now)
	require.NoError(t, err)
	require.NoError(t, l2.Release(h.ctx))
}

func TestLeasePerSourceIndependent(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()
	l1, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "source-a", time.Minute, time.Now)
	require.NoError(t, err)
	l2, err := acquireLease(h.ctx, h.backend, controlID, h.targetID, "source-b", time.Minute, time.Now)
	require.NoError(t, err)
	require.NotEqual(t, leaseKeyPrefix(h.targetID, "x"), l1.object)
	require.NotEqual(t, l1.object, l2.object)
	require.NoError(t, l1.Release(h.ctx))
	require.NoError(t, l2.Release(h.ctx))
}

func stringsRepeated(s string, n int) string {
	b := make([]byte, 0, n)
	for i := 0; i < n; i++ {
		b = append(b, s[0])
	}
	return string(b)
}
