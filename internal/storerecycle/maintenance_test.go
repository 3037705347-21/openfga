package storerecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openfga/openfga/pkg/storage"
	fgmemory "github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
	rcmemory "github.com/openfga/openfga/pkg/storage/storerecycle/memory"
)

func TestSweepStaleThenRecoverSucceeds(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)

	now := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	mutableNow := now
	svc := New(live, rcmemory.New(), WithClock(func() time.Time { return mutableNow }))
	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}

	// Interrupt a recovery mid-copy: the session is left RUNNING and its
	// staging handle has been abandoned.
	blocking := &blockingStaging{blockEntered: make(chan struct{})}
	svc.newStaging = func() (storage.OpenFGADatastore, error) {
		blocking.OpenFGADatastore = fgmemory.New()
		return blocking, nil
	}
	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Recover(runCtx, fx.storeID)
		errCh <- err
	}()
	<-blocking.blockEntered
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	// Startup sweep: stale sessions are discarded, entry returns to RECYCLED.
	swept, err := svc.SweepStale(ctx)
	if err != nil {
		t.Fatalf("SweepStale: %v", err)
	}
	if len(swept) != 1 || swept[0].Status != storerecycle.RecoveryDiscarded ||
		swept[0].DiscardReason != staleRecoveryReason {
		t.Fatalf("unexpected sweep result: %+v", swept)
	}
	if len(svc.stagings) != 0 {
		t.Fatalf("no staging handles may survive a sweep")
	}
	sessions, _ := svc.ListRecoveries(ctx, fx.storeID)
	if len(sessions) != 1 || sessions[0].Status != storerecycle.RecoveryDiscarded {
		t.Fatalf("session must be DISCARDED, got %+v", sessions)
	}
	entry, _ := svc.GetRecycleEntry(ctx, fx.storeID)
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("entry must return to RECYCLED, got %s", entry.Status)
	}

	// Sweep is idempotent once everything is terminal.
	again, err := svc.SweepStale(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("second sweep must be empty, got %+v err=%v", again, err)
	}

	// A brand-new recovery (fresh factory, fresh staging) succeeds.
	svc.newStaging = func() (storage.OpenFGADatastore, error) { return fgmemory.New(), nil }
	recovered, err := svc.Recover(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("recovery after sweep: %v", err)
	}
	if recovered.Status != storerecycle.RecoveryCompleted {
		t.Fatalf("recovery status: %s", recovered.Status)
	}
	assertRestoredContent(t, ctx, live, fx, entry.Manifest)
}

func TestServicePurgeExpiredGate(t *testing.T) {
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)

	current := time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC)
	svc := New(live, rcmemory.New(),
		WithClock(func() time.Time { return current }),
		WithRetention(time.Hour),
	)
	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}

	// Before expiry: retained.
	if purged, err := svc.PurgeExpired(ctx); err != nil || len(purged) != 0 {
		t.Fatalf("nothing must be purged before expiry, got %v err=%v", purged, err)
	}

	// Expired but with an active (interrupted) recovery: still retained.
	blocking := &blockingStaging{blockEntered: make(chan struct{})}
	svc.newStaging = func() (storage.OpenFGADatastore, error) {
		blocking.OpenFGADatastore = fgmemory.New()
		return blocking, nil
	}
	current = current.Add(2 * time.Hour) // entry is now expired
	runCtx, cancel := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Recover(runCtx, fx.storeID)
		errCh <- err
	}()
	<-blocking.blockEntered

	if purged, err := svc.PurgeExpired(ctx); err != nil || len(purged) != 0 {
		t.Fatalf("active recovery must block purge even when expired, got %v err=%v", purged, err)
	}
	if _, err := svc.GetRecycleEntry(ctx, fx.storeID); err != nil {
		t.Fatalf("entry must still exist: %v", err)
	}

	cancel()
	<-errCh
	if _, err := svc.SweepStale(ctx); err != nil {
		t.Fatalf("SweepStale: %v", err)
	}

	// Terminal session: the expired entry is now purgeable; takeover records
	// are left untouched.
	purged, err := svc.PurgeExpired(ctx)
	if err != nil || len(purged) != 1 || purged[0] != fx.storeID {
		t.Fatalf("expected the expired entry to be purged, got %v err=%v", purged, err)
	}
	if _, err := svc.GetRecycleEntry(ctx, fx.storeID); !errors.Is(err, storerecycle.ErrEntryNotFound) {
		t.Fatalf("entry must be gone, got %v", err)
	}
}
