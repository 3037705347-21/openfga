package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

var t0 = time.Unix(1_700_000_000, 0).UTC()

func testEntry(storeID string, status storerecycle.EntryStatus, expiresAt time.Time) *storerecycle.Entry {
	return &storerecycle.Entry{
		ID:        "entry-" + storeID,
		StoreID:   storeID,
		StoreName: "name-" + storeID,
		Status:    status,
		DeletedAt: t0,
		ExpiresAt: expiresAt,
		Manifest: storerecycle.Manifest{
			ModelsCount: 1,
		},
		CreatedAt: t0,
		UpdatedAt: t0,
	}
}

func testSnapshot(storeID string) *storerecycle.Snapshot {
	return &storerecycle.Snapshot{
		Store:  &openfgav1.Store{Id: storeID, Name: "name-" + storeID},
		Models: []*openfgav1.AuthorizationModel{{Id: "01", SchemaVersion: "1.1"}},
		Tuples: []*openfgav1.TupleKey{{Object: "document:1", Relation: "viewer", User: "user:a"}},
		Assertions: map[string][]*openfgav1.Assertion{
			"01": {{TupleKey: &openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:a"}, Expectation: true}},
		},
	}
}

func putRecycled(t *testing.T, b *Backend, storeID string, expiresAt time.Time) {
	t.Helper()
	entry := testEntry(storeID, storerecycle.EntryRecycled, expiresAt)
	if err := b.PutEntry(context.Background(), entry, testSnapshot(storeID)); err != nil {
		t.Fatalf("PutEntry: %v", err)
	}
}

func runningSession(id string) *storerecycle.RecoverySession {
	return &storerecycle.RecoverySession{
		ID:             id,
		StagingStoreID: "staging-" + id,
		Status:         storerecycle.RecoveryRunning,
		StartedAt:      t0,
		HeartbeatAt:    t0,
		UpdatedAt:      t0,
		Progress:       storerecycle.Progress{Phase: storerecycle.PhaseCopy, ModelsTotal: 1, TuplesTotal: 1},
	}
}

func TestPutAndGetSnapshotDeepCopy(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))

	entry, err := b.GetEntry(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("unexpected status %s", entry.Status)
	}

	snap1, err := b.GetSnapshot(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	snap1.Store.Name = "mutated"
	snap1.Tuples[0].Object = "document:999"
	snap1.Models[0].Id = "99"

	snap2, err := b.GetSnapshot(context.Background(), "s1")
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snap2.Store.GetName() != "name-s1" {
		t.Fatalf("stored snapshot mutated through returned copy: %q", snap2.Store.GetName())
	}
	if snap2.Tuples[0].GetObject() != "document:1" || snap2.Models[0].GetId() != "01" {
		t.Fatalf("stored snapshot content leaked: %+v / %+v", snap2.Tuples[0], snap2.Models[0])
	}

	if _, err := b.GetEntry(context.Background(), "missing"); !errors.Is(err, storerecycle.ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound, got %v", err)
	}
}

func TestPutEntryRejectsRecovering(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))

	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r1")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}

	err := b.PutEntry(context.Background(), testEntry("s1", storerecycle.EntryRecycled, t0.Add(2*time.Hour)), testSnapshot("s1"))
	if !errors.Is(err, storerecycle.ErrRecoveryActive) {
		t.Fatalf("expected ErrRecoveryActive, got %v", err)
	}
}

func TestInitRecoveryGating(t *testing.T) {
	b := New()

	if _, err := b.InitRecovery(context.Background(), "missing", runningSession("r1")); !errors.Is(err, storerecycle.ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound, got %v", err)
	}

	putRecycled(t, b, "s1", t0.Add(time.Hour))
	got, err := b.InitRecovery(context.Background(), "s1", runningSession("r1"))
	if err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	if got.Status != storerecycle.RecoveryRunning || got.StoreID != "s1" || got.EntryID != "entry-s1" {
		t.Fatalf("unexpected session: %+v", got)
	}

	entry, _ := b.GetEntry(context.Background(), "s1")
	if entry.Status != storerecycle.EntryRecovering {
		t.Fatalf("entry must be RECOVERING, got %s", entry.Status)
	}

	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r2")); !errors.Is(err, storerecycle.ErrRecoveryActive) {
		t.Fatalf("expected ErrRecoveryActive, got %v", err)
	}

	// A RESTORED entry cannot be recovered again; a new generation must be put first.
	if _, err := b.CompleteRecovery(context.Background(), "s1", "r1", t0.Add(time.Minute)); err != nil {
		t.Fatalf("CompleteRecovery: %v", err)
	}
	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r3")); !errors.Is(err, storerecycle.ErrEntryNotRecycled) {
		t.Fatalf("expected ErrEntryNotRecycled, got %v", err)
	}
	if _, err := b.GetSnapshot(context.Background(), "s1"); !errors.Is(err, storerecycle.ErrSnapshotUnavailable) {
		t.Fatalf("expected ErrSnapshotUnavailable, got %v", err)
	}
}

func TestInitRecoverySingleWinner(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))

	const n = 32
	var wg sync.WaitGroup
	results := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := b.InitRecovery(context.Background(), "s1", runningSession("r"+string(rune('a'+i))))
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)

	winners, active := 0, 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, storerecycle.ErrRecoveryActive):
			active++
		default:
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if winners != 1 || active != n-1 {
		t.Fatalf("expected exactly 1 winner, got winners=%d active=%d", winners, active)
	}
}

func TestFailRecoveryAllowsRetry(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))

	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r1")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	if _, err := b.UpdateRecovery(context.Background(), "s1", "r1", func(s *storerecycle.RecoverySession) error {
		s.Progress.ModelsCopied = 1
		return nil
	}); err != nil {
		t.Fatalf("UpdateRecovery: %v", err)
	}

	failed, err := b.FailRecovery(context.Background(), "s1", "r1", "verify mismatch", t0.Add(time.Minute))
	if err != nil {
		t.Fatalf("FailRecovery: %v", err)
	}
	if failed.Status != storerecycle.RecoveryFailed || failed.FailReason != "verify mismatch" {
		t.Fatalf("unexpected failed session: %+v", failed)
	}
	entry, _ := b.GetEntry(context.Background(), "s1")
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("entry must return to RECYCLED, got %s", entry.Status)
	}

	// Terminal sessions cannot be updated, but a fresh attempt can start.
	if _, err := b.UpdateRecovery(context.Background(), "s1", "r1", func(s *storerecycle.RecoverySession) error {
		return nil
	}); !errors.Is(err, storerecycle.ErrRecoveryNotActive) {
		t.Fatalf("expected ErrRecoveryNotActive, got %v", err)
	}
	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r2")); err != nil {
		t.Fatalf("retry after failure: %v", err)
	}

	sessions, err := b.ListRecoveries(context.Background(), "s1")
	if err != nil || len(sessions) != 2 || sessions[0].ID != "r2" || sessions[1].ID != "r1" {
		t.Fatalf("expected newest-first sessions [r2 r1], got %+v err=%v", sessions, err)
	}
}

func TestDiscardStale(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))
	putRecycled(t, b, "s2", t0.Add(time.Hour))

	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r1")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	// s2 stays RECYCLED with no session.

	affected, err := b.DiscardStale(context.Background(), "restart", t0.Add(time.Minute))
	if err != nil {
		t.Fatalf("DiscardStale: %v", err)
	}
	if len(affected) != 1 || affected[0].ID != "r1" || affected[0].Status != storerecycle.RecoveryDiscarded ||
		affected[0].DiscardReason != "restart" {
		t.Fatalf("unexpected affected sessions: %+v", affected)
	}

	entry, _ := b.GetEntry(context.Background(), "s1")
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("s1 must return to RECYCLED, got %s", entry.Status)
	}
	session, _ := b.GetRecovery(context.Background(), "s1", "r1")
	if session.Status != storerecycle.RecoveryDiscarded {
		t.Fatalf("r1 must be DISCARDED, got %s", session.Status)
	}

	// Idempotent: nothing stale left.
	again, err := b.DiscardStale(context.Background(), "restart", t0.Add(2*time.Minute))
	if err != nil || len(again) != 0 {
		t.Fatalf("second sweep must be empty, got %+v err=%v", again, err)
	}

	// A discarded attempt permits a fresh recovery.
	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r2")); err != nil {
		t.Fatalf("recovery after discard: %v", err)
	}
}

func TestPurgeExpiredMatrix(t *testing.T) {
	b := New()
	hour := time.Hour

	putRecycled(t, b, "fresh", t0.Add(hour))       // not expired
	putRecycled(t, b, "expired", t0.Add(-hour))    // expired, no active session
	putRecycled(t, b, "recovering", t0.Add(-hour)) // expired but RECOVERING
	putRecycled(t, b, "restored", t0.Add(-2*hour)) // RESTORED, expired (must be retained)

	if _, err := b.InitRecovery(context.Background(), "recovering", runningSession("r1")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	if _, err := b.InitRecovery(context.Background(), "restored", runningSession("rX")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	if _, err := b.CompleteRecovery(context.Background(), "restored", "rX", t0); err != nil {
		t.Fatalf("CompleteRecovery: %v", err)
	}

	// A takeover record on the expired entry must survive the purge.
	if err := b.AppendTakeover(context.Background(), &storerecycle.TakeoverRecord{
		ID: "t1", StoreID: "expired", SessionID: "old", Result: storerecycle.TakeoverSucceeded,
		StartedAt: t0, CompletedAt: t0,
	}); err != nil {
		t.Fatalf("AppendTakeover: %v", err)
	}

	now := t0
	purged, err := b.PurgeExpired(context.Background(), now)
	if err != nil {
		t.Fatalf("PurgeExpired: %v", err)
	}
	if len(purged) != 1 || purged[0].StoreID != "expired" {
		t.Fatalf("only the expired inactive entry must be purged, got %+v", purged)
	}

	if _, err := b.GetEntry(context.Background(), "expired"); !errors.Is(err, storerecycle.ErrEntryNotFound) {
		t.Fatalf("purged entry must be gone, got %v", err)
	}
	for _, id := range []string{"fresh", "recovering", "restored"} {
		if _, err := b.GetEntry(context.Background(), id); err != nil {
			t.Fatalf("entry %s must be retained: %v", id, err)
		}
	}

	records, err := b.ListTakeovers(context.Background(), "expired")
	if err != nil || len(records) != 1 || records[0].ID != "t1" {
		t.Fatalf("takeover record must survive purge, got %+v err=%v", records, err)
	}

	// Once the active recovery ends in failure, the expired entry becomes purgeable.
	if _, err := b.FailRecovery(context.Background(), "recovering", "r1", "boom", now); err != nil {
		t.Fatalf("FailRecovery: %v", err)
	}
	purged2, err := b.PurgeExpired(context.Background(), now)
	if err != nil || len(purged2) != 1 || purged2[0].StoreID != "recovering" {
		t.Fatalf("recovering entry must purge after session ends, got %+v err=%v", purged2, err)
	}

	// RESTORED and non-expired are still there.
	again, err := b.PurgeExpired(context.Background(), now)
	if err != nil || len(again) != 0 {
		t.Fatalf("nothing more must be purgeable, got %+v err=%v", again, err)
	}
}

func TestReturnedCopiesAreIndependent(t *testing.T) {
	b := New()
	putRecycled(t, b, "s1", t0.Add(time.Hour))
	if _, err := b.InitRecovery(context.Background(), "s1", runningSession("r1")); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}

	s, err := b.GetRecovery(context.Background(), "s1", "r1")
	if err != nil {
		t.Fatalf("GetRecovery: %v", err)
	}
	s.Status = storerecycle.RecoveryCompleted
	s.Progress.ModelsCopied = 42

	again, err := b.GetRecovery(context.Background(), "s1", "r1")
	if err != nil {
		t.Fatalf("GetRecovery: %v", err)
	}
	if again.Status != storerecycle.RecoveryRunning || again.Progress.ModelsCopied != 0 {
		t.Fatalf("internal state leaked through returned session: %+v", again)
	}

	entries, err := b.ListEntries(context.Background())
	if err != nil || len(entries) != 1 {
		t.Fatalf("ListEntries: %d err=%v", len(entries), err)
	}
	entries[0].Status = storerecycle.EntryRestored
	if got, _ := b.GetEntry(context.Background(), "s1"); got.Status != storerecycle.EntryRecovering {
		t.Fatalf("internal entry leaked through list copy: %s", got.Status)
	}

	if _, err := b.GetRecovery(context.Background(), "s1", "nope"); !errors.Is(err, storerecycle.ErrRecoveryNotFound) {
		t.Fatalf("expected ErrRecoveryNotFound, got %v", err)
	}
}
