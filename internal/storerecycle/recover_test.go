package storerecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	fgmemory "github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
	rcmemory "github.com/openfga/openfga/pkg/storage/storerecycle/memory"
)

func recycleThenRecoverSetup(t *testing.T, svcOpts ...Option) (context.Context, fixture, storage.OpenFGADatastore, *Service, *rcmemory.Backend) {
	t.Helper()
	ctx := context.Background()
	fx := newFixture()
	live := fgmemory.New()
	seedLiveStore(t, ctx, live, fx)

	backend := rcmemory.New()
	opts := append([]Option{WithClock(time.Now), WithModelPageSize(1)}, svcOpts...)
	svc := New(live, backend, opts...)
	if err := svc.Recycle(ctx, fx.storeID); err != nil {
		t.Fatalf("Recycle: %v", err)
	}
	return ctx, fx, live, svc, backend
}

// droppingStaging wraps a staging datastore and silently drops writes whose
// tuple key matches the predicate.
type droppingStaging struct {
	storage.OpenFGADatastore
	drop func(*openfgav1.TupleKey) bool
}

func (d *droppingStaging) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	kept := make(storage.Writes, 0, len(writes))
	for _, tk := range writes {
		if !d.drop(tk) {
			kept = append(kept, tk)
		}
	}
	return d.OpenFGADatastore.Write(ctx, store, deletes, kept, opts...)
}

// blockingStaging blocks the first non-empty tuple Write until ctx is done,
// simulating a crash interrupted mid-copy.
type blockingStaging struct {
	storage.OpenFGADatastore
	blockedOnce  atomic.Bool
	blockEntered chan struct{}
}

func (b *blockingStaging) Write(
	ctx context.Context,
	store string,
	deletes storage.Deletes,
	writes storage.Writes,
	opts ...storage.TupleWriteOption,
) error {
	if len(writes) > 0 && b.blockedOnce.CompareAndSwap(false, true) {
		close(b.blockEntered)
		select {
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return b.OpenFGADatastore.Write(ctx, store, deletes, writes, opts...)
}

func assertRestoredContent(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, fx fixture, expected storerecycle.Manifest) {
	t.Helper()

	store, err := ds.GetStore(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetStore after recovery: %v", err)
	}
	if store.GetName() != "documents" {
		t.Fatalf("store name: %q", store.GetName())
	}

	snap, err := enumerateSnapshot(ctx, ds, store, expected.CapturedAt, 50)
	if err != nil {
		t.Fatalf("enumerate restored content: %v", err)
	}
	got := snap.Manifest
	if got.ModelsCount != expected.ModelsCount ||
		got.ModelsFingerprint != expected.ModelsFingerprint ||
		got.LatestModelID != expected.LatestModelID ||
		got.TuplesCount != expected.TuplesCount ||
		got.TuplesFingerprint != expected.TuplesFingerprint ||
		got.AssertionModels != expected.AssertionModels ||
		got.AssertionsCount != expected.AssertionsCount ||
		got.AssertionsFingerprint != expected.AssertionsFingerprint {
		t.Fatalf("restored manifest mismatch:\nwant %+v\ngot  %+v", expected, got)
	}

	latest, err := ds.FindLatestAuthorizationModel(ctx, fx.storeID)
	if err != nil || latest.GetId() != fx.newModelID {
		t.Fatalf("latest model after recovery: %v err=%v", latest, err)
	}

	// Condition round-trips through the full cycle.
	var condOK bool
	for _, tk := range snap.Tuples {
		if tk.GetObject() == "document:2" && tk.GetRelation() == "editor" &&
			tk.GetCondition().GetName() == "in_business_hours" &&
			tk.GetCondition().GetContext().AsMap()["hours"] == 9.0 {
			condOK = true
		}
	}
	if !condOK {
		t.Fatalf("condition tuple did not survive restore")
	}
}

func TestRecoverEndToEnd(t *testing.T) {
	ctx, fx, live, svc, backend := recycleThenRecoverSetup(t)

	entry, err := backend.GetEntry(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetEntry: %v", err)
	}
	expectedManifest := entry.Manifest
	snapshotBefore, err := backend.GetSnapshot(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetSnapshot: %v", err)
	}
	originalCreatedAt := snapshotBefore.Store.GetCreatedAt().AsTime()

	// The original id is invisible throughout recovery preparation.
	if _, err := live.GetStore(ctx, fx.storeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("recycled store must be invisible before takeover, got %v", err)
	}

	time.Sleep(2 * time.Millisecond) // guarantee UpdatedAt strictly follows CreatedAt
	session, err := svc.Recover(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if session.Status != storerecycle.RecoveryCompleted {
		t.Fatalf("session status: %s", session.Status)
	}

	assertRestoredContent(t, ctx, live, fx, expectedManifest)

	restored, err := live.GetStore(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if !restored.GetCreatedAt().AsTime().Equal(originalCreatedAt) {
		t.Fatalf("CreatedAt must be preserved: got %v want %v", restored.GetCreatedAt(), originalCreatedAt)
	}
	if !restored.GetUpdatedAt().AsTime().After(restored.GetCreatedAt().AsTime()) {
		t.Fatalf("UpdatedAt must be refreshed at takeover: created=%v updated=%v",
			restored.GetCreatedAt(), restored.GetUpdatedAt())
	}

	// Entry is RESTORED and payload released.
	entry, _ = backend.GetEntry(ctx, fx.storeID)
	if entry.Status != storerecycle.EntryRestored {
		t.Fatalf("entry status: %s", entry.Status)
	}
	if _, err := backend.GetSnapshot(ctx, fx.storeID); !errors.Is(err, storerecycle.ErrSnapshotUnavailable) {
		t.Fatalf("snapshot payload must be released, got %v", err)
	}

	// A SUCCESS takeover record exists.
	takeovers, err := svc.ListTakeovers(ctx, fx.storeID)
	if err != nil || len(takeovers) != 1 || takeovers[0].Result != storerecycle.TakeoverSucceeded {
		t.Fatalf("unexpected takeover records: %+v err=%v", takeovers, err)
	}
	if takeovers[0].Manifest.TuplesCount != expectedManifest.TuplesCount ||
		takeovers[0].StagingStoreID == "" || takeovers[0].SessionID != session.ID {
		t.Fatalf("takeover record incomplete: %+v", takeovers[0])
	}

	// New writes are accepted and immediately visible.
	if err := live.Write(ctx, fx.storeID, nil, []*openfgav1.TupleKey{
		{Object: "document:3", Relation: "viewer", User: "user:dave"},
	}); err != nil {
		t.Fatalf("Write after restore: %v", err)
	}
	if tuples := listLiveTuples(t, ctx, live, fx.storeID); len(tuples) != 4 {
		t.Fatalf("expected 4 tuples after new write, got %d", len(tuples))
	}
}

func TestRecoverProgressAndPhases(t *testing.T) {
	ctx, fx, _, svc, backend := recycleThenRecoverSetup(t)

	var mu sync.Mutex
	var phases []storerecycle.RecoveryPhase
	var prev storerecycle.Progress
	monotonic := true
	svc.observe = func(p storerecycle.Progress) {
		mu.Lock()
		defer mu.Unlock()
		if len(phases) == 0 || phases[len(phases)-1] != p.Phase {
			phases = append(phases, p.Phase)
		}
		if p.ModelsCopied < prev.ModelsCopied || p.TuplesCopied < prev.TuplesCopied ||
			p.ModelsVerified < prev.ModelsVerified {
			monotonic = false
		}
		prev = p
	}

	session, err := svc.Recover(ctx, fx.storeID)
	if err != nil {
		t.Fatalf("Recover: %v", err)
	}

	wantPhases := []storerecycle.RecoveryPhase{
		storerecycle.PhaseCopy, storerecycle.PhaseVerify, storerecycle.PhaseTakeover,
	}
	if fmt.Sprint(phases) != fmt.Sprint(wantPhases) {
		t.Fatalf("observed phases: %v want %v", phases, wantPhases)
	}
	if !monotonic {
		t.Fatalf("progress counters must be monotonic")
	}

	final, err := backend.GetRecovery(ctx, session.StoreID, session.ID)
	if err != nil {
		t.Fatalf("GetRecovery: %v", err)
	}
	p := final.Progress
	if p.ModelsCopied != p.ModelsTotal || p.TuplesCopied != p.TuplesTotal ||
		p.AssertionModelsCopied != p.AssertionModelsTotal || p.AssertionsCopied != p.AssertionsTotal ||
		p.ModelsVerified != p.ModelsTotal || p.TuplesVerified != p.TuplesTotal {
		t.Fatalf("final progress incomplete: %+v", p)
	}
}

func TestRecoverSingleFlight(t *testing.T) {
	ctx, fx, _, svc, _ := recycleThenRecoverSetup(t)

	const n = 8
	var wg sync.WaitGroup
	completed := make(chan *storerecycle.RecoverySession, n)
	rejected := make(chan error, n)
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sess, err := svc.Recover(ctx, fx.storeID)
			if err != nil {
				rejected <- err
				return
			}
			completed <- sess
		}()
	}
	close(start)
	wg.Wait()
	close(completed)
	close(rejected)

	winners := 0
	for sess := range completed {
		if sess.Status == storerecycle.RecoveryCompleted {
			winners++
		}
	}
	busy := 0
	for err := range rejected {
		if errors.Is(err, storerecycle.ErrRecoveryActive) {
			busy++
		} else {
			t.Fatalf("unexpected loser error: %v", err)
		}
	}
	if winners != 1 || busy != n-1 {
		t.Fatalf("want 1 winner and %d rejections, got winners=%d busy=%d", n-1, winners, busy)
	}
}

func TestRecoverVerificationFailureDiscardsStagingAndRetrySucceeds(t *testing.T) {
	ctx, fx, live, svc, backend := recycleThenRecoverSetup(t)

	// Replace the staging factory with one that drops document:1#viewer@alice.
	var dropEnabled atomic.Bool
	dropEnabled.Store(true)
	svc.newStaging = func() (storage.OpenFGADatastore, error) {
		return &droppingStaging{
			OpenFGADatastore: fgmemory.New(),
			drop: func(tk *openfgav1.TupleKey) bool {
				return dropEnabled.Load() && tk.GetObject() == "document:1" &&
					tk.GetRelation() == "viewer" && tk.GetUser() == "user:alice"
			},
		}, nil
	}

	sessionsBefore, _ := backend.ListRecoveries(ctx, fx.storeID)
	if len(sessionsBefore) != 0 {
		t.Fatalf("expected no sessions, got %d", len(sessionsBefore))
	}

	_, err := svc.Recover(ctx, fx.storeID)
	if err == nil {
		t.Fatalf("expected verification failure")
	}
	var mismatchErr *storerecycle.VerificationMismatchError
	if !errors.As(err, &mismatchErr) || !errors.Is(err, storerecycle.ErrVerificationFailed) {
		t.Fatalf("expected VerificationMismatchError wrapping ErrVerificationFailed, got %v", err)
	}
	foundTupleCount := false
	for _, m := range mismatchErr.Mismatches {
		if m.Kind == "tuples" && m.Property == "count" && m.Expected == "3" && m.Actual == "2" {
			foundTupleCount = true
		}
	}
	if !foundTupleCount {
		t.Fatalf("expected tuples count 3!=2 mismatch, got %+v", mismatchErr.Mismatches)
	}

	// Original id stays invisible; session FAILED; entry back to RECYCLED; the
	// staging handle was discarded.
	if _, err := live.GetStore(ctx, fx.storeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("original id must stay invisible after failed recovery, got %v", err)
	}
	sessions, _ := backend.ListRecoveries(ctx, fx.storeID)
	if len(sessions) != 1 || sessions[0].Status != storerecycle.RecoveryFailed {
		t.Fatalf("expected one FAILED session, got %+v", sessions)
	}
	entry, _ := backend.GetEntry(ctx, fx.storeID)
	if entry.Status != storerecycle.EntryRecycled {
		t.Fatalf("entry must return to RECYCLED, got %s", entry.Status)
	}
	if len(svc.stagings) != 0 {
		t.Fatalf("staging handle must be discarded on failure, got %d", len(svc.stagings))
	}

	// No takeover record when the pipeline never reached TAKEOVER.
	if records, _ := svc.ListTakeovers(ctx, fx.storeID); len(records) != 0 {
		t.Fatalf("failed verification must not produce takeover records, got %d", len(records))
	}

	// Retry with a healthy staging factory recovers the full content.
	dropEnabled.Store(false)
	if _, err := svc.Recover(ctx, fx.storeID); err != nil {
		t.Fatalf("retry recovery: %v", err)
	}
	expected := entry.Manifest
	assertRestoredContent(t, ctx, live, fx, expected)
}

func TestRecoverTakeoverConflictKeepsLiveStoreUntouched(t *testing.T) {
	ctx, fx, live, svc, backend := recycleThenRecoverSetup(t)

	// An impostor recreated the original id while the store was in the bin.
	impostor := &openfgav1.Store{
		Id:        fx.storeID,
		Name:      "impostor",
		CreatedAt: timestamppb.New(time.Now()),
		UpdatedAt: timestamppb.New(time.Now()),
	}
	if _, err := live.CreateStore(ctx, impostor); err != nil {
		t.Fatalf("CreateStore impostor: %v", err)
	}

	_, err := svc.Recover(ctx, fx.storeID)
	if !errors.Is(err, storerecycle.ErrStoreAlreadyActive) {
		t.Fatalf("expected ErrStoreAlreadyActive, got %v", err)
	}

	got, _ := live.GetStore(ctx, fx.storeID)
	if got.GetName() != "impostor" {
		t.Fatalf("live store must not be overwritten, got %q", got.GetName())
	}
	sessions, _ := backend.ListRecoveries(ctx, fx.storeID)
	if len(sessions) != 1 || sessions[0].Status != storerecycle.RecoveryFailed {
		t.Fatalf("expected one FAILED session, got %+v", sessions)
	}
	takeovers, _ := svc.ListTakeovers(ctx, fx.storeID)
	if len(takeovers) != 1 || takeovers[0].Result != storerecycle.TakeoverFailed ||
		takeovers[0].Error == "" {
		t.Fatalf("expected one FAILED takeover record with reason, got %+v", takeovers)
	}

	// Once the impostor is removed, a new recovery succeeds and records SUCCESS.
	if err := any(live).(storerecycle.StorePurger).PurgeStore(ctx, fx.storeID); err != nil {
		t.Fatalf("PurgeStore impostor: %v", err)
	}
	if _, err := svc.Recover(ctx, fx.storeID); err != nil {
		t.Fatalf("recovery after conflict: %v", err)
	}
	takeovers, _ = svc.ListTakeovers(ctx, fx.storeID)
	if len(takeovers) != 2 || takeovers[0].Result != storerecycle.TakeoverFailed ||
		takeovers[1].Result != storerecycle.TakeoverSucceeded {
		t.Fatalf("expected FAILED then SUCCESS takeover records, got %+v", takeovers)
	}
	entry, _ := backend.GetEntry(ctx, fx.storeID)
	assertRestoredContent(t, ctx, live, fx, entry.Manifest)
}

func TestRecoverInterruptedByContextStaysRunning(t *testing.T) {
	ctx, fx, _, svc, backend := recycleThenRecoverSetup(t)

	blocking := &blockingStaging{blockEntered: make(chan struct{})}
	svc.newStaging = func() (storage.OpenFGADatastore, error) {
		blocking.OpenFGADatastore = fgmemory.New()
		return blocking, nil
	}

	runCtx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.Recover(runCtx, fx.storeID)
		errCh <- err
	}()

	<-blocking.blockEntered
	cancel()

	err := <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	sessions, _ := backend.ListRecoveries(ctx, fx.storeID)
	if len(sessions) != 1 || sessions[0].Status != storerecycle.RecoveryRunning {
		t.Fatalf("interrupted session must stay RUNNING, got %+v", sessions)
	}
	if len(svc.stagings) != 0 {
		t.Fatalf("abandoned staging handle must be released")
	}
}
