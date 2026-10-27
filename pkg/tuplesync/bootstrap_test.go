package tuplesync

import (
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
)

func TestEnsureControlStoreCreatesAndIsIdempotent(t *testing.T) {
	h := newTestHarness(t)
	controlID := h.ensureControlStore()

	first := controlID
	afterCreate := len(h.allTuples(first))
	require.Greater(t, afterCreate, 0, "version marker must be written")

	// repeat: same store, no additional writes.
	second, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(h.targetID))
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Len(t, h.allTuples(first), afterCreate, "idempotent Ensure must not write again")

	storedModel, err := h.backend.GetAuthorizationModel(h.ctx, first, "")
	require.NoError(t, err)
	require.NoError(t, verifyControlModel(storedModel))
}

func TestEnsureControlStoreDetectsModelDrift(t *testing.T) {
	t.Run("changed type set on latest model", func(t *testing.T) {
		h := newTestHarness(t)
		controlID := h.ensureControlStore()

		drifted := parser.MustTransformDSLToProto(`
model
  schema 1.1

type fga_source
type fga_claim
  relations
    define owner: [fga_source]
`)
		drifted.Id = ulid.Make().String()
		require.NoError(t, h.ds.WriteAuthorizationModel(h.ctx, controlID, drifted))

		_, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(h.targetID))
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})

	t.Run("removed version marker", func(t *testing.T) {
		h := newTestHarness(t)
		controlID := h.ensureControlStore()
		require.NoError(t, h.ds.Write(h.ctx, controlID,
			[]*openfgav1.TupleKeyWithoutCondition{
				{Object: sysControlObject, Relation: relVersion, User: controlSourceType + ":" + CodecVersion},
			}, nil))

		_, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(h.targetID))
		require.ErrorIs(t, err, ErrLedgerUnverifiable)
	})
}

func TestEnsureControlStoreCompletesInterruptedBootstrap(t *testing.T) {
	h := newTestHarness(t)
	targetID, _ := h.createTargetStore()

	// Simulate a crash between CreateStore and WriteAuthorizationModel.
	storeID := ulid.Make().String()
	_, err := h.ds.CreateStore(h.ctx, &openfgav1.Store{Id: storeID, Name: DefaultControlStoreName})
	require.NoError(t, err)

	controlID, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(targetID))
	require.NoError(t, err)
	require.Equal(t, storeID, controlID)
	require.NotEmpty(t, h.allTuples(controlID))

	// Idempotent afterwards.
	again, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, h.targetModel(targetID))
	require.NoError(t, err)
	require.Equal(t, storeID, again)
}

func TestEnsureControlStoreRejectsReservedTypeInTargetModel(t *testing.T) {
	h := newTestHarness(t)
	evil := parser.MustTransformDSLToProto(`
model
  schema 1.1

type user
type fga_claim
  relations
    define owner: [user]
`)
	_, err := EnsureControlStore(h.ctx, h.backend, DefaultControlStoreName, evil)
	require.ErrorIs(t, err, ErrReservedTypeCollision)
}

func TestFindStoreByNameAmbiguity(t *testing.T) {
	h := newTestHarness(t)
	_, err := h.ds.CreateStore(h.ctx, &openfgav1.Store{Id: ulid.Make().String(), Name: "dup"})
	require.NoError(t, err)
	_, err = h.ds.CreateStore(h.ctx, &openfgav1.Store{Id: ulid.Make().String(), Name: "dup"})
	require.NoError(t, err)

	_, err = h.backend.FindStoreByName(h.ctx, "dup")
	require.ErrorIs(t, err, ErrAmbiguousControlStore)

	missing, err := h.backend.FindStoreByName(h.ctx, "nope")
	require.NoError(t, err)
	require.Nil(t, missing)
}

func TestDatastoreBackendRejectsUnscopedReads(t *testing.T) {
	h := newTestHarness(t)
	require.ErrorContains(t, h.backend.ReadByUser(h.ctx, "store", "", nil), "non-empty user")
	require.ErrorContains(t, h.backend.ReadObject(h.ctx, "store", "", nil), "non-empty object")
}

func TestDatastoreBackendTupleReadWrite(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	key := &openfgav1.TupleKeyWithoutCondition{Object: "document:1", Relation: "viewer", User: "user:alice"}

	_, err := h.backend.ReadTuple(h.ctx, storeID, key)
	require.ErrorIs(t, err, ErrTupleNotFound)

	require.NoError(t, h.backend.WriteBatch(h.ctx, storeID, nil,
		[]*openfgav1.TupleKey{{Object: key.Object, Relation: key.Relation, User: key.User}}, false))

	tp, err := h.backend.ReadTuple(h.ctx, storeID, key)
	require.NoError(t, err)
	require.Equal(t, "document:1", tp.GetKey().GetObject())

	// idempotent re-insert and idempotent delete of a missing key.
	require.NoError(t, h.backend.WriteBatch(h.ctx, storeID, nil,
		[]*openfgav1.TupleKey{{Object: key.Object, Relation: key.Relation, User: key.User}}, true))
	require.NoError(t, h.backend.WriteBatch(h.ctx, storeID,
		[]*openfgav1.TupleKeyWithoutCondition{{Object: "document:2", Relation: "viewer", User: "user:ghost"}}, nil, true))
}

func TestDatastoreBackendReadByUserAndObject(t *testing.T) {
	h := newTestHarness(t)
	storeID, _ := h.createTargetStore()
	h.writeTargetTuples(storeID,
		&openfgav1.TupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		&openfgav1.TupleKey{Object: "document:2", Relation: "viewer", User: "user:alice"},
		&openfgav1.TupleKey{Object: "document:2", Relation: "editor", User: "user:bob"},
	)

	var byUser []string
	require.NoError(t, h.backend.ReadByUser(h.ctx, storeID, "user:alice", func(tp *openfgav1.Tuple) error {
		byUser = append(byUser, tp.GetKey().GetObject())
		return nil
	}))
	require.ElementsMatch(t, []string{"document:1", "document:2"}, byUser)

	var byObject []string
	require.NoError(t, h.backend.ReadObject(h.ctx, storeID, "document:2", func(tp *openfgav1.Tuple) error {
		byObject = append(byObject, tp.GetKey().GetRelation())
		return nil
	}))
	require.ElementsMatch(t, []string{"viewer", "editor"}, byObject)
}
