package storerecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	fgmemory "github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
	rcmemory "github.com/openfga/openfga/pkg/storage/storerecycle/memory"
)

func newTestService(t *testing.T, live storage.OpenFGADatastore, clock func() time.Time) (*Service, *rcmemory.Backend) {
	t.Helper()
	backend := rcmemory.New()
	svc := New(live, backend,
		WithClock(clock),
		WithRetention(48*time.Hour),
		WithModelPageSize(1), // force multi-page model enumeration
	)
	return svc, backend
}

func TestRecycleHappyPath(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)

	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	svc, backend := newTestService(t, live, func() time.Time { return now })

	liveStore, err := live.GetStore(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetStore before recycle: %v", err)
	}

	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}

	assertLiveStoreAbsent(t, ctx, live, fx.storeID)
	if tuples := listLiveTuples(t, ctx, live, fx.storeID); len(tuples) != 0 {
		t.Fatalf("tuples must be hard-purged, got %d", len(tuples))
	}
	if _, _, err := live.ReadChanges(ctx, fx.storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("changelog must be purged, got %v", err)
	}

	entry, err := backend.GetEntry(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Status != storerecycle.EntryRecycled || entry.StoreName != "documents" {
		t.Fatalf("unexpected entry: %+v", entry)
	}
	if !entry.DeletedAt.Equal(now) || !entry.ExpiresAt.Equal(now.Add(48*time.Hour)) {
		t.Fatalf("unexpected timestamps: deleted=%v expires=%v", entry.DeletedAt, entry.ExpiresAt)
	}
	m := entry.Manifest
	if m.ModelsCount != 2 || m.TuplesCount != 3 || m.AssertionModels != 2 || m.AssertionsCount != 3 {
		t.Fatalf("unexpected manifest counts: %+v", m)
	}
	if m.ModelsFingerprint == "" || m.TuplesFingerprint == "" || m.AssertionsFingerprint == "" {
		t.Fatalf("fingerprints must be populated: %+v", m)
	}
	if m.LatestModelID != fx.newModelID {
		t.Fatalf("latest model: want %s got %s", fx.newModelID, m.LatestModelID)
	}

	snapshot, err := backend.GetSnapshot(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if len(snapshot.Models) != 2 || len(snapshot.Tuples) != 3 || len(snapshot.Assertions) != 2 {
		t.Fatalf("unexpected snapshot sizes: %+v", snapshot)
	}
	if !snapshot.Store.GetCreatedAt().AsTime().Equal(liveStore.GetCreatedAt().AsTime()) {
		t.Fatalf("CreatedAt must be captured: snapshot=%v live=%v",
			snapshot.Store.GetCreatedAt(), liveStore.GetCreatedAt())
	}
	var foundCondition bool
	for _, tk := range snapshot.Tuples {
		if tk.GetObject() == "document:2" && tk.GetRelation() == "editor" {
			if tk.GetCondition().GetName() != "in_business_hours" ||
				tk.GetCondition().GetContext().AsMap()["hours"] != 9.0 {
				t.Fatalf("condition not captured: %+v", tk.GetCondition())
			}
			foundCondition = true
		}
	}
	if !foundCondition {
		t.Fatalf("condition tuple missing from snapshot")
	}
}

func TestRecycleSnapshotIsImmutableAfterPurge(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)
	svc, backend := newTestService(t, live, func() time.Time {
		return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	})

	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}

	// Recreate the same id with unrelated content; the snapshot must not move.
	if _, err := live.CreateStore(ctx, &openfgav1.Store{Id: fx.storeID, Name: "impostor"}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}
	if err := live.Write(ctx, fx.storeID, nil, []*openfgav1.TupleKey{
		{Object: "other:1", Relation: "viewer", User: "user:zzz"},
	}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	snapshot, err := backend.GetSnapshot(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	if snapshot.Store.GetName() != "documents" {
		t.Fatalf("snapshot store header changed: %q", snapshot.Store.GetName())
	}
	if snapshot.Manifest.TuplesCount != 3 || len(snapshot.Tuples) != 3 {
		t.Fatalf("snapshot tuples changed: %+v", snapshot.Manifest)
	}

	// Returned snapshots are independent of stored state.
	snapshot.Tuples[0].Object = "mutated:1"
	again, _ := backend.GetSnapshot(ctx, fx.storeID)
	if again.Tuples[0].GetObject() == "mutated:1" {
		t.Fatalf("backend handed out mutable snapshot state")
	}
}

func TestRecycleMissingStoreIsIdempotent(t *testing.T) {
	ctx := context.Background()
	live := fgmemory.New()
	svc, _ := newTestService(t, live, time.Now)

	if err := svc.Recycle(ctx, "01HDOESNOTEXIST0000000000"); err != nil {
		t.Fatalf("recycling a missing store must succeed, got %v", err)
	}
}

func TestRecycleBlockedWhileRecoveryActive(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)
	svc, backend := newTestService(t, live, time.Now)

	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if _, err := backend.InitRecovery(ctx, fx.storeID, &storerecycle.RecoverySession{
		ID:        "r1",
		Status:    storerecycle.RecoveryRunning,
		StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}

	// With the entry RECOVERING, another recycle must be rejected by the gate.
	if err := svc.Recycle(ctx, fx.storeID); !errors.Is(err, storerecycle.ErrRecoveryActive) {
		t.Fatalf("expected ErrRecoveryActive, got %v", err)
	}
}

func TestRecycleCreatesNewGenerationAfterRestore(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)
	svc, backend := newTestService(t, live, time.Now)

	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if _, err := backend.InitRecovery(ctx, fx.storeID, &storerecycle.RecoverySession{
		ID: "r1", Status: storerecycle.RecoveryRunning, StartedAt: time.Now(),
	}); err != nil {
		t.Fatalf("InitRecovery: %v", err)
	}
	if _, err := backend.CompleteRecovery(ctx, fx.storeID, "r1", time.Now()); err != nil {
		t.Fatalf("CompleteRecovery: %v", err)
	}
	if _, err := backend.GetSnapshot(ctx, fx.storeID); !errors.Is(err, storerecycle.ErrSnapshotUnavailable) {
		t.Fatalf("expected ErrSnapshotUnavailable, got %v", err)
	}

	// Simulate the real pipeline's end state: the store is live again under
	// the original id. A subsequent delete then produces a fresh generation.
	seedLiveStore(t, ctx, live, fx)
	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle after restore: %v", err)
	}
	entry, err := backend.GetEntry(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("new generation must be RECYCLED, got %s", entry.Status)
	}
	snapshot, err := backend.GetSnapshot(ctx, fx.storeID)
	if err != nil || snapshot.Manifest.TuplesCount != 3 {
		t.Fatalf("new generation must carry a fresh snapshot, got %v err=%v", snapshot, err)
	}
}

func TestRecycleFallsBackToLegacyDeleteStore(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	raw := fgmemory.New()
	seedLiveStore(t, ctx, raw, fx)

	wrapped := &deleteCountingDatastore{standardOnlyDatastore: standardOnlyDatastore{raw}}
	if _, ok := any(wrapped).(storerecycle.StorePurger); ok {
		t.Fatalf("wrapper must hide StorePurger")
	}

	svc := New(wrapped, rcmemory.New(), WithClock(time.Now))
	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	if wrapped.deletes != 1 {
		t.Fatalf("legacy DeleteStore must be called exactly once, got %d", wrapped.deletes)
	}
	if _, err := raw.GetStore(ctx, fx.storeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("expected store gone after legacy delete, got %v", err)
	}
}
