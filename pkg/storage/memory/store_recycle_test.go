package memory

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

func drainTuples(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, storeID string) []*openfgav1.Tuple {
	t.Helper()
	it, err := ds.Read(ctx, storeID, storage.ReadFilter{}, storage.ReadOptions{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer it.Stop()

	var out []*openfgav1.Tuple
	for {
		tuple, err := it.Next(ctx)
		if errors.Is(err, storage.ErrIteratorDone) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, tuple)
	}
}

func seedRecycleableStore(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, storeID string) *storerecycle.InstallPayload {
	t.Helper()

	created := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	store := &openfgav1.Store{
		Id:        storeID,
		Name:      "doomed",
		CreatedAt: timestamppb.New(created),
		UpdatedAt: timestamppb.New(created),
	}
	if _, err := ds.CreateStore(ctx, store); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	oldModel := &openfgav1.AuthorizationModel{
		Id:              "01HV0000000000000000000001",
		SchemaVersion:   "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}},
	}
	newModel := &openfgav1.AuthorizationModel{
		Id:              "01HV0000000000000000000002",
		SchemaVersion:   "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}, {Type: "document"}},
	}
	if err := ds.WriteAuthorizationModel(ctx, storeID, oldModel); err != nil {
		t.Fatalf("WriteAuthorizationModel old: %v", err)
	}
	if err := ds.WriteAuthorizationModel(ctx, storeID, newModel); err != nil {
		t.Fatalf("WriteAuthorizationModel new: %v", err)
	}

	condCtx, err := structpb.NewStruct(map[string]any{"hours": 42.0})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	writes := []*openfgav1.TupleKey{
		{Object: "document:1", Relation: "viewer", User: "user:alice"},
		{Object: "document:2", Relation: "viewer", User: "user:bob",
			Condition: &openfgav1.RelationshipCondition{Name: "in_business_hours", Context: condCtx}},
	}
	if err := ds.Write(ctx, storeID, nil, writes); err != nil {
		t.Fatalf("Write: %v", err)
	}

	asserts := []*openfgav1.Assertion{{
		TupleKey:    &openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		Expectation: true,
	}}
	if err := ds.WriteAssertions(ctx, storeID, newModel.GetId(), asserts); err != nil {
		t.Fatalf("WriteAssertions: %v", err)
	}

	return &storerecycle.InstallPayload{
		Store:  store,
		Models: []*openfgav1.AuthorizationModel{oldModel, newModel},
		Tuples: writes,
		Assertions: map[string][]*openfgav1.Assertion{
			newModel.GetId(): asserts,
		},
	}
}

func assertStoreFullyAbsent(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, storeID string) {
	t.Helper()

	if _, err := ds.GetStore(ctx, storeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetStore after purge: expected ErrNotFound, got %v", err)
	}

	models, _, err := ds.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	})
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("ReadAuthorizationModels after purge: %v", err)
	}
	if len(models) != 0 {
		t.Fatalf("models must be purged, got %d", len(models))
	}

	if tuples := drainTuples(t, ctx, ds, storeID); len(tuples) != 0 {
		t.Fatalf("tuples must be purged, got %d", len(tuples))
	}

	if _, _, err := ds.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("changelog must be purged, got err=%v", err)
	}

	got, err := ds.ReadAssertions(ctx, storeID, "any-model")
	if err != nil {
		t.Fatalf("ReadAssertions after purge: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("assertions must be purged, got %d", len(got))
	}
}

func TestPurgeStoreRemovesEverythingAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	ds := New().(*MemoryBackend)
	payload := seedRecycleableStore(t, ctx, ds, "01HPURGE00000000000000000")

	purger, ok := any(ds).(storerecycle.StorePurger)
	if !ok {
		t.Fatalf("MemoryBackend must implement StorePurger")
	}

	if err := purger.PurgeStore(ctx, payload.Store.GetId()); err != nil {
		t.Fatalf("PurgeStore: %v", err)
	}
	assertStoreFullyAbsent(t, ctx, ds, payload.Store.GetId())

	// Idempotent.
	if err := purger.PurgeStore(ctx, payload.Store.GetId()); err != nil {
		t.Fatalf("second PurgeStore: %v", err)
	}
}

func TestInstallStoreRestoresContentAtomically(t *testing.T) {
	ctx := context.Background()
	ds := New().(*MemoryBackend)
	payload := seedRecycleableStore(t, ctx, ds, "01HINSTALL0000000000000000")

	installer := any(ds).(storerecycle.StoreInstaller)
	purger := any(ds).(storerecycle.StorePurger)
	if err := purger.PurgeStore(ctx, payload.Store.GetId()); err != nil {
		t.Fatalf("PurgeStore: %v", err)
	}

	// Mutate the payload's timestamps to exercise preserving CreatedAt while
	// setting a fresh UpdatedAt at takeover time.
	payload.Store.UpdatedAt = timestamppb.New(time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC))

	if err := installer.InstallStore(ctx, *payload); err != nil {
		t.Fatalf("InstallStore: %v", err)
	}

	store, err := ds.GetStore(ctx, payload.Store.GetId())
	if err != nil {
		t.Fatalf("GetStore after install: %v", err)
	}
	if store.GetName() != "doomed" {
		t.Fatalf("store name not restored: %q", store.GetName())
	}
	if store.GetCreatedAt().AsTime() != time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC) {
		t.Fatalf("CreatedAt must be preserved, got %v", store.GetCreatedAt())
	}
	if store.GetUpdatedAt().AsTime() != time.Date(2025, 6, 7, 8, 9, 10, 0, time.UTC) {
		t.Fatalf("UpdatedAt must come from payload, got %v", store.GetUpdatedAt())
	}

	models, _, err := ds.ReadAuthorizationModels(ctx, payload.Store.GetId(), storage.ReadAuthorizationModelsOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	})
	if err != nil {
		t.Fatalf("ReadAuthorizationModels: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 model versions, got %d", len(models))
	}
	latest, err := ds.FindLatestAuthorizationModel(ctx, payload.Store.GetId())
	if err != nil {
		t.Fatalf("FindLatestAuthorizationModel: %v", err)
	}
	if latest.GetId() != "01HV0000000000000000000002" {
		t.Fatalf("latest model must be the newer version, got %s", latest.GetId())
	}

	tuples := drainTuples(t, ctx, ds, payload.Store.GetId())
	if len(tuples) != 2 {
		t.Fatalf("expected 2 tuples, got %d", len(tuples))
	}
	keys := make([]string, 0, len(tuples))
	var foundCond bool
	for _, tuple := range tuples {
		tk := tuple.GetKey()
		keys = append(keys, tk.GetObject()+"#"+tk.GetRelation()+"@"+tk.GetUser())
		if tk.GetObject() == "document:2" {
			if tk.GetCondition().GetName() != "in_business_hours" {
				t.Fatalf("condition name lost: %q", tk.GetCondition().GetName())
			}
			if tk.GetCondition().GetContext().AsMap()["hours"] != 42.0 {
				t.Fatalf("condition context lost: %v", tk.GetCondition().GetContext().AsMap())
			}
			foundCond = true
		}
	}
	sort.Strings(keys)
	if keys[0] != "document:1#viewer@user:alice" || keys[1] != "document:2#viewer@user:bob" || !foundCond {
		t.Fatalf("unexpected tuple set: %+v cond=%v", keys, foundCond)
	}

	gotAsserts, err := ds.ReadAssertions(ctx, payload.Store.GetId(), "01HV0000000000000000000002")
	if err != nil {
		t.Fatalf("ReadAssertions: %v", err)
	}
	if len(gotAsserts) != 1 || gotAsserts[0].GetTupleKey().GetObject() != "document:1" || !gotAsserts[0].GetExpectation() {
		t.Fatalf("assertions not restored: %+v", gotAsserts)
	}

	// Changelog history is intentionally not synthesized.
	if _, _, err := ds.ReadChanges(ctx, payload.Store.GetId(), storage.ReadChangesFilter{}, storage.ReadChangesOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	}); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("restored store must have no historical changelog, got err=%v", err)
	}

	// The restored store accepts new writes immediately.
	err = ds.Write(ctx, payload.Store.GetId(), nil, []*openfgav1.TupleKey{
		{Object: "document:3", Relation: "viewer", User: "user:carol"},
	})
	if err != nil {
		t.Fatalf("Write after restore: %v", err)
	}
	if tuples := drainTuples(t, ctx, ds, payload.Store.GetId()); len(tuples) != 3 {
		t.Fatalf("expected 3 tuples after new write, got %d", len(tuples))
	}
}

func TestInstallStoreCollisionWritesNothing(t *testing.T) {
	ctx := context.Background()
	ds := New().(*MemoryBackend)
	payload := seedRecycleableStore(t, ctx, ds, "01HCOLLIDE000000000000000")

	installer := any(ds).(storerecycle.StoreInstaller)

	// Store is still live from seeding: installing must collide.
	err := installer.InstallStore(ctx, *payload)
	if !errors.Is(err, storage.ErrCollision) {
		t.Fatalf("expected ErrCollision, got %v", err)
	}

	// Existing content must be untouched.
	store, err := ds.GetStore(ctx, payload.Store.GetId())
	if err != nil || store.GetName() != "doomed" {
		t.Fatalf("live store changed after failed install: %v err=%v", store, err)
	}
	if tuples := drainTuples(t, ctx, ds, payload.Store.GetId()); len(tuples) != 2 {
		t.Fatalf("tuples must remain unchanged after failed install, got %d", len(tuples))
	}

	// An empty store id is rejected before any lock effects.
	empty := storerecycle.InstallPayload{Store: &openfgav1.Store{Id: ""}}
	if err := installer.InstallStore(ctx, empty); err == nil {
		t.Fatalf("expected error for payload without store id")
	}
}

func TestInstallStoreDeepCopiesPayload(t *testing.T) {
	ctx := context.Background()
	ds := New().(*MemoryBackend)
	payload := seedRecycleableStore(t, ctx, ds, "01HISOLATION00000000000000")
	if err := any(ds).(storerecycle.StorePurger).PurgeStore(ctx, payload.Store.GetId()); err != nil {
		t.Fatalf("PurgeStore: %v", err)
	}
	if err := any(ds).(storerecycle.StoreInstaller).InstallStore(ctx, *payload); err != nil {
		t.Fatalf("InstallStore: %v", err)
	}

	// Mutate every payload object after installation.
	payload.Store.Name = "mutated"
	payload.Models[0].TypeDefinitions[0].Type = "mutated"
	payload.Tuples[0].Object = "document:999"
	payload.Tuples[1].GetCondition().GetContext().AsMap()["hours"] = 1.0
	for _, asserts := range payload.Assertions {
		asserts[0].Expectation = false
	}

	store, _ := ds.GetStore(ctx, payload.Store.GetId())
	if store.GetName() != "doomed" {
		t.Fatalf("stored store header leaked through payload: %q", store.GetName())
	}
	models, _, _ := ds.ReadAuthorizationModels(ctx, payload.Store.GetId(), storage.ReadAuthorizationModelsOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	})
	for _, m := range models {
		for _, td := range m.GetTypeDefinitions() {
			if td.GetType() == "mutated" {
				t.Fatalf("stored model leaked through payload")
			}
		}
	}
	for _, tuple := range drainTuples(t, ctx, ds, payload.Store.GetId()) {
		if tuple.GetKey().GetObject() == "document:999" {
			t.Fatalf("stored tuple leaked through payload")
		}
		if tuple.GetKey().GetObject() == "document:2" &&
			tuple.GetKey().GetCondition().GetContext().AsMap()["hours"] != 42.0 {
			t.Fatalf("stored condition context leaked through payload")
		}
	}
	for modelID := range payload.Assertions {
		got, _ := ds.ReadAssertions(ctx, payload.Store.GetId(), modelID)
		for _, a := range got {
			if a.GetExpectation() != true {
				t.Fatalf("stored assertion leaked through payload")
			}
		}
	}
}
