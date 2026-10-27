package tuplesync

import (
	"context"
	"errors"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// ErrTupleNotFound is returned by Backend.ReadTuple when the exact tuple is
// absent. Backend implementations translate their native not-found errors to
// it.
var ErrTupleNotFound = errors.New("tuple not found")

// Backend is the transport-narrow surface the planner and applier use.
// It is implemented once over storage.OpenFGADatastore for in-process use and
// once over gRPC for the CLI; all reconciliation logic is shared.
//
// Reads are deliberately scoped: there is intentionally no "read all tuples
// in a store" method, so a sync can never perform a full target-store scan.
type Backend interface {
	// ReadTuple fetches one exact tuple. Returns ErrTupleNotFound if absent.
	ReadTuple(ctx context.Context, store string, key *openfgav1.TupleKeyWithoutCondition) (*openfgav1.Tuple, error)

	// ReadByUser invokes fn for every tuple whose user field equals user.
	// user must be non-empty.
	ReadByUser(ctx context.Context, store, user string, fn func(*openfgav1.Tuple) error) error

	// ReadObject invokes fn for every tuple on the exact object
	// ("type:id"). object must be non-empty.
	ReadObject(ctx context.Context, store, object string, fn func(*openfgav1.Tuple) error) error

	// WriteBatch applies one transactional write (deletes before writes).
	// When idempotent is true, duplicate inserts and missing deletes are
	// ignored; this is required by the crash-recovery protocol.
	WriteBatch(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes, idempotent bool) error

	// MaxBatchSize is the backend limit for deletes+writes in one WriteBatch.
	MaxBatchSize() int

	// GetAuthorizationModel fetches a model; modelID "" means the store's
	// latest model.
	GetAuthorizationModel(ctx context.Context, store, modelID string) (*openfgav1.AuthorizationModel, error)

	// FindStoreByName returns the uniquely-named store, or nil when none
	// exists. Multiple stores sharing the name is a hard error (store names
	// are not unique at the API level).
	FindStoreByName(ctx context.Context, name string) (*openfgav1.Store, error)

	// CreateStoreWithModel creates a store, provisions the given model on it
	// and applies extraWrites (for example the codec version marker) as part
	// of bootstrap. It returns the new store id and model id.
	CreateStoreWithModel(ctx context.Context, name string, model *openfgav1.AuthorizationModel, extraWrites []*openfgav1.TupleKey) (storeID, modelID string, err error)

	// ProvisionModelOnStore writes a model (and optional extra tuples) onto
	// an already created store. It is used only to complete an interrupted
	// bootstrap (store created, model missing).
	ProvisionModelOnStore(ctx context.Context, storeID string, model *openfgav1.AuthorizationModel, extraWrites []*openfgav1.TupleKey) (modelID string, err error)
}
