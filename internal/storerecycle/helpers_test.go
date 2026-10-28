package storerecycle

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

type fixture struct {
	storeID    string
	oldModelID string
	newModelID string
	tupleKeys  []*openfgav1.TupleKey
	assertions map[string][]*openfgav1.Assertion
	createdAt  time.Time
}

func newFixture() fixture {
	ctxStruct, _ := structpb.NewStruct(map[string]any{"hours": 9.0})
	return fixture{
		storeID:    "01HRECYCLE000000000000001",
		oldModelID: "01HMODEL0000000000000001",
		newModelID: "01HMODEL0000000000000002",
		tupleKeys: []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:alice"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "document:2", Relation: "editor", User: "user:carol",
				Condition: &openfgav1.RelationshipCondition{Name: "in_business_hours", Context: ctxStruct}},
		},
		assertions: map[string][]*openfgav1.Assertion{
			"01HMODEL0000000000000001": {{
				TupleKey:    &openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
				Expectation: true,
			}},
			"01HMODEL0000000000000002": {
				{
					TupleKey:    &openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
					Expectation: true,
				},
				{
					TupleKey:    &openfgav1.AssertionTupleKey{Object: "document:2", Relation: "editor", User: "user:carol"},
					Expectation: false,
				},
			},
		},
		createdAt: time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC),
	}
}

// seedLiveStore creates a populated live store and returns the fixture that
// describes exactly what was written.
func seedLiveStore(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, fx fixture) {
	t.Helper()

	if _, err := ds.CreateStore(ctx, &openfgav1.Store{
		Id:        fx.storeID,
		Name:      "documents",
		CreatedAt: timestamppb.New(fx.createdAt),
		UpdatedAt: timestamppb.New(fx.createdAt),
	}); err != nil {
		t.Fatalf("CreateStore: %v", err)
	}

	oldModel := &openfgav1.AuthorizationModel{
		Id:              fx.oldModelID,
		SchemaVersion:   "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}},
	}
	newModel := &openfgav1.AuthorizationModel{
		Id:            fx.newModelID,
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "document"},
		},
	}
	if err := ds.WriteAuthorizationModel(ctx, fx.storeID, oldModel); err != nil {
		t.Fatalf("WriteAuthorizationModel old: %v", err)
	}
	if err := ds.WriteAuthorizationModel(ctx, fx.storeID, newModel); err != nil {
		t.Fatalf("WriteAuthorizationModel new: %v", err)
	}

	if err := ds.Write(ctx, fx.storeID, nil, fx.tupleKeys); err != nil {
		t.Fatalf("Write tuples: %v", err)
	}

	for modelID, asserts := range fx.assertions {
		if err := ds.WriteAssertions(ctx, fx.storeID, modelID, asserts); err != nil {
			t.Fatalf("WriteAssertions %s: %v", modelID, err)
		}
	}
}

func listLiveTuples(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, storeID string) []*openfgav1.Tuple {
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

func assertLiveStoreAbsent(t *testing.T, ctx context.Context, ds storage.OpenFGADatastore, storeID string) {
	t.Helper()
	if _, err := ds.GetStore(ctx, storeID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("GetStore: expected ErrNotFound, got %v", err)
	}
	stores, _, err := ds.ListStores(ctx, storage.ListStoresOptions{
		Pagination: storage.NewPaginationOptions(50, ""),
	})
	if err != nil {
		t.Fatalf("ListStores: %v", err)
	}
	for _, st := range stores {
		if st.GetId() == storeID {
			t.Fatalf("recycled store must not appear in ListStores")
		}
	}
}

// standardOnlyDatastore hides the optional StorePurger/StoreInstaller
// capabilities by promoting only the OpenFGADatastore interface methods.
type standardOnlyDatastore struct {
	storage.OpenFGADatastore
}

// deleteCountingDatastore additionally records legacy DeleteStore calls.
type deleteCountingDatastore struct {
	standardOnlyDatastore
	deletes int
}

func (d *deleteCountingDatastore) DeleteStore(ctx context.Context, id string) error {
	d.deletes++
	return d.OpenFGADatastore.DeleteStore(ctx, id)
}
