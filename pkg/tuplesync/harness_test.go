package tuplesync

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
)

// testBusinessDSL is the target-store authorization model used across the
// tuplesync tests. It covers plain users, usersets and an ABAC condition.
const testBusinessDSL = `
model
  schema 1.1

type user

type group
  relations
    define member: [user, group#member]

type document
  relations
    define viewer: [user, user with inOffice, group#member]
    define editor: [user, group#member]

condition inOffice(floor: int, wing: int) {
  floor == 3
}
`

type testHarness struct {
	t             *testing.T
	ds            storage.OpenFGADatastore
	backend       *DatastoreBackend
	ctx           context.Context
	targetID      string
	targetModelID string
}

func newTestHarness(t *testing.T) *testHarness {
	t.Helper()
	ds := memory.New()
	h := &testHarness{
		t:       t,
		ds:      ds,
		backend: NewDatastoreBackend(ds),
		ctx:     context.Background(),
	}
	return h
}

// createTargetStore provisions a target store with testBusinessDSL and
// returns its id and model id.
func (h *testHarness) createTargetStore() (string, string) {
	h.t.Helper()
	storeID := ulid.Make().String()
	_, err := h.ds.CreateStore(h.ctx, &openfgav1.Store{Id: storeID, Name: "target"})
	require.NoError(h.t, err)
	model := parser.MustTransformDSLToProto(testBusinessDSL)
	model.Id = ulid.Make().String()
	require.NoError(h.t, h.ds.WriteAuthorizationModel(h.ctx, storeID, model))
	h.targetID = storeID
	h.targetModelID = model.GetId()
	return storeID, model.GetId()
}

func (h *testHarness) targetModel(storeID string) *openfgav1.AuthorizationModel {
	h.t.Helper()
	m, err := h.backend.GetAuthorizationModel(h.ctx, storeID, "")
	require.NoError(h.t, err)
	return m
}

// writeTargetTuples writes tuples directly into the target store, bypassing
// the syncer (simulating manual grants / test fixtures).
func (h *testHarness) writeTargetTuples(storeID string, keys ...*openfgav1.TupleKey) {
	h.t.Helper()
	require.NoError(h.t, h.ds.Write(h.ctx, storeID, nil, keys))
}

// allTuples returns every tuple in a store (test-only full scan).
func (h *testHarness) allTuples(storeID string) []*openfgav1.Tuple {
	h.t.Helper()
	iter, err := h.ds.Read(h.ctx, storeID, storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(h.t, err)
	defer iter.Stop()
	var out []*openfgav1.Tuple
	for {
		tp, err := iter.Next(h.ctx)
		if err == storage.ErrIteratorDone {
			return out
		}
		require.NoError(h.t, err)
		out = append(out, tp)
	}
}

func (h *testHarness) tupleExists(storeID string, object, relation, user string) bool {
	h.t.Helper()
	_, err := h.backend.ReadTuple(h.ctx, storeID, &openfgav1.TupleKeyWithoutCondition{
		Object: object, Relation: relation, User: user,
	})
	if err == nil {
		return true
	}
	require.ErrorIs(h.t, err, ErrTupleNotFound)
	return false
}

func (h *testHarness) ensureControlStoreFor(targetID string) string {
	h.t.Helper()
	id, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(targetID))
	require.NoError(h.t, err)
	return id
}

func (h *testHarness) ensureControlStore() string {
	h.t.Helper()
	if h.targetID == "" {
		h.createTargetStore()
	}
	controlID, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(h.targetID))
	require.NoError(h.t, err)
	return controlID
}
