package server

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
	rcmemory "github.com/openfga/openfga/pkg/storage/storerecycle/memory"
)

func TestServerDeleteStoreUsesRecycleBinWhenConfigured(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctx := context.Background()
	ds := memory.New()
	t.Cleanup(ds.Close)

	recycleBackend := rcmemory.New()
	srv := MustNewServerWithOpts(
		WithDatastore(ds),
		WithStoreRecycleBackend(recycleBackend),
	)
	t.Cleanup(srv.Close)

	recovery := srv.StoreRecovery()
	require.NotNil(t, recovery, "recycle service must be exposed when configured")

	// Create a store with one model, tuples and assertions.
	created, err := srv.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "documents"})
	require.NoError(t, err)
	storeID := created.GetId()

	model := &openfgav1.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}, {Type: "document"}},
	}
	require.NoError(t, ds.WriteAuthorizationModel(ctx, storeID, model))
	require.NoError(t, ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "document:1", Relation: "viewer", User: "user:alice"},
	}))
	require.NoError(t, ds.WriteAssertions(ctx, storeID, model.GetId(), []*openfgav1.Assertion{{
		TupleKey:    &openfgav1.AssertionTupleKey{Object: "document:1", Relation: "viewer", User: "user:alice"},
		Expectation: true,
	}}))

	// Delete through the server: it lands in the recycle bin, not a hard/legacy
	// delete.
	_, err = srv.DeleteStore(ctx, &openfgav1.DeleteStoreRequest{StoreId: storeID})
	require.NoError(t, err)

	_, err = srv.GetStore(ctx, &openfgav1.GetStoreRequest{StoreId: storeID})
	require.Error(t, err)
	require.Equal(t, codes.Code(openfgav1.NotFoundErrorCode_store_id_not_found), status.Code(err),
		"recycled store must be invisible")

	entries, err := recovery.ListRecycleBin(ctx)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, storeID, entries[0].StoreID)
	require.Equal(t, "documents", entries[0].StoreName)
	require.Equal(t, storerecycle.EntryRecycled, entries[0].Status)
	require.Equal(t, 1, entries[0].Manifest.ModelsCount)
	require.Equal(t, 1, entries[0].Manifest.TuplesCount)

	// Deleting the now-missing id again stays a successful no-op.
	_, err = srv.DeleteStore(ctx, &openfgav1.DeleteStoreRequest{StoreId: storeID})
	require.NoError(t, err)

	// Drive recovery through the Go service API and observe the store live
	// again under the same id, visible through the server.
	session, err := recovery.Recover(ctx, storeID)
	require.NoError(t, err)
	require.Equal(t, storerecycle.RecoveryCompleted, session.Status)

	got, err := srv.GetStore(ctx, &openfgav1.GetStoreRequest{StoreId: storeID})
	require.NoError(t, err)
	require.Equal(t, "documents", got.GetName())

	// The full content survived the round trip.
	it, err := ds.Read(ctx, storeID, storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(t, err)
	tuple, err := it.Head(ctx)
	require.NoError(t, err)
	require.Equal(t, "document:1", tuple.GetKey().GetObject())
	it.Stop()

	asserts, err := ds.ReadAssertions(ctx, storeID, model.GetId())
	require.NoError(t, err)
	require.Len(t, asserts, 1)

	takeovers, err := recovery.ListTakeovers(ctx, storeID)
	require.NoError(t, err)
	require.Len(t, takeovers, 1)
	require.Equal(t, storerecycle.TakeoverSucceeded, takeovers[0].Result)
}

func TestServerDeleteStoreKeepsLegacyPathWithoutRecycleBackend(t *testing.T) {
	t.Cleanup(func() { goleak.VerifyNone(t) })

	ctx := context.Background()
	ds := memory.New()
	t.Cleanup(ds.Close)

	srv := MustNewServerWithOpts(WithDatastore(ds))
	t.Cleanup(srv.Close)

	require.Nil(t, srv.StoreRecovery(), "recycle service must stay nil without the option")

	created, err := srv.CreateStore(ctx, &openfgav1.CreateStoreRequest{Name: "legacy"})
	require.NoError(t, err)

	_, err = srv.DeleteStore(ctx, &openfgav1.DeleteStoreRequest{StoreId: created.GetId()})
	require.NoError(t, err)

	_, err = srv.GetStore(ctx, &openfgav1.GetStoreRequest{StoreId: created.GetId()})
	require.Equal(t, codes.Code(openfgav1.NotFoundErrorCode_store_id_not_found), status.Code(err))
}
