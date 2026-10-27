package tuplesync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// DefaultLeaseTTL is the lease duration used when no WithLeaseTTL option is
// given. It only guards concurrent runs; a crashed holder is recoverable once
// the TTL elapses.
const DefaultLeaseTTL = 5 * time.Minute

// leaseActiveUser is the fixed user on every lease tuple. The run identity
// lives in the object id, so concurrent attempts insert distinct tuples and
// elect a deterministic winner (smallest fresh run ULID) rather than relying
// on the datastore to reject multi-valued relation users.
const leaseActiveUser = controlRunType + ":active"

// ErrSyncInProgress means another run currently holds the (store, source)
// lease and its holder ULID is still within TTL.
var ErrSyncInProgress = errors.New("sync already in progress")

// Lease serializes sync/adopt runs for one (target store, source).
type Lease struct {
	b         Backend
	controlID string
	object    string
	runULID   ulid.ULID
	now       func() time.Time
}

// leaseKeyPrefix is the per-(store, source) prefix of attempt objects.
func leaseKeyPrefix(targetStoreID, source string) string {
	sum := sha256.Sum256([]byte(source))
	return fmt.Sprintf("%s:%s:%s-", controlLeaseType, targetStoreID, hex.EncodeToString(sum[:]))
}

func leaseAttemptObject(targetStoreID, source, runULID string) string {
	return leaseKeyPrefix(targetStoreID, source) + runULID
}

// acquireLease acquires the lease. Every attempt inserts a distinct tuple and
// the smallest run ULID that is still inside TTL deterministically wins, so
// exactly one concurrent acquirer proceeds; stale (crashed) holders lose and
// are cleaned up.
func acquireLease(ctx context.Context, b Backend, controlID, targetStoreID, source string, ttl time.Duration, now func() time.Time) (*Lease, error) {
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	if now == nil {
		now = time.Now
	}
	prefix := leaseKeyPrefix(targetStoreID, source)

	// Snapshot before registering: any fresh holder that already exists
	// blocks unconditionally (a running sync is not a candidate for
	// election).
	pre, err := readAttempts(ctx, b, controlID, prefix)
	if err != nil {
		return nil, err
	}
	for objectID, id := range pre {
		if now().Sub(ulid.Time(id.Time())) <= ttl {
			return nil, fmt.Errorf("%w: lease prefix %s is held by run %s until %s",
				ErrSyncInProgress, prefix, id.String(),
				ulid.Time(id.Time()).Add(ttl).Format(time.RFC3339))
		}
	}

	run := ulid.Make()
	own := leaseAttemptObject(targetStoreID, source, run.String())
	if err := b.WriteBatch(ctx, controlID, nil, []*openfgav1.TupleKey{{
		Object: own, Relation: relHolder, User: leaseActiveUser,
	}}, false); err != nil {
		return nil, fmt.Errorf("register lease attempt: %w", err)
	}

	l := &Lease{b: b, controlID: controlID, object: own, runULID: run, now: now}

	// Re-read to arbitrate against attempts that registered concurrently.
	attempts, err := readAttempts(ctx, b, controlID, prefix)
	if err != nil {
		return nil, err
	}

	var stale []string
	var freshULIDs []ulid.ULID
	for objectID, id := range attempts {
		if now().Sub(ulid.Time(id.Time())) <= ttl {
			freshULIDs = append(freshULIDs, id)
			continue
		}
		stale = append(stale, objectID)
	}
	sort.Slice(freshULIDs, func(i, j int) bool { return freshULIDs[i].Compare(freshULIDs[j]) < 0 })

	// Clean up crashed holders; never fatal if best-effort cleanup races.
	_ = deleteLeaseObjects(ctx, b, controlID, stale)

	if len(freshULIDs) > 0 && freshULIDs[0].Compare(run) != 0 {
		// A concurrent, deterministically-earlier attempt wins. Withdraw.
		_ = l.Release(ctx)
		winner := freshULIDs[0]
		return nil, fmt.Errorf("%w: lease prefix %s is held by run %s until %s",
			ErrSyncInProgress, prefix, winner.String(),
			ulid.Time(winner.Time()).Add(ttl).Format(time.RFC3339))
	}
	return l, nil
}

// Release removes this run's lease tuple. It is idempotent.
func (l *Lease) Release(ctx context.Context) error {
	return l.b.WriteBatch(ctx, l.controlID,
		[]*openfgav1.TupleKeyWithoutCondition{{
			Object: l.object, Relation: relHolder, User: leaseActiveUser,
		}}, nil, true)
}

// readAttempts returns attempt object-id -> run ULID for one (store, source)
// lease prefix, enumerated via the fixed lease user (no type-prefix scans).
func readAttempts(ctx context.Context, b Backend, controlID, prefix string) (map[string]ulid.ULID, error) {
	out := map[string]ulid.ULID{}
	err := b.ReadByUser(ctx, controlID, leaseActiveUser, func(tp *openfgav1.Tuple) error {
		object := tp.GetKey().GetObject()
		if tp.GetKey().GetRelation() != relHolder || !strings.HasPrefix(object, prefix) {
			return nil
		}
		run, ok := strings.CutPrefix(object, prefix)
		if !ok {
			return nil
		}
		id, err := ulid.Parse(run)
		if err != nil {
			return fmt.Errorf("%w: lease object %q carries unparseable run id", ErrLedgerUnverifiable, object)
		}
		out[object] = id
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func deleteLeaseObjects(ctx context.Context, b Backend, controlID string, objects []string) error {
	if len(objects) == 0 {
		return nil
	}
	deletes := make([]*openfgav1.TupleKeyWithoutCondition, 0, len(objects))
	for _, o := range objects {
		deletes = append(deletes, &openfgav1.TupleKeyWithoutCondition{
			Object: o, Relation: relHolder, User: leaseActiveUser,
		})
	}
	return b.WriteBatch(ctx, controlID, deletes, nil, true)
}
