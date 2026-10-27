package tuplesync

import (
	"context"
	"errors"
	"fmt"

	"github.com/oklog/ulid/v2"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
)

// DatastoreBackend adapts storage.OpenFGADatastore to the Backend interface.
// Use it to embed tuplesync in the same process as an OpenFGA datastore
// (memory/postgres/mysql/sqlite).
type DatastoreBackend struct {
	ds storage.OpenFGADatastore
}

// NewDatastoreBackend wraps an OpenFGADatastore.
func NewDatastoreBackend(ds storage.OpenFGADatastore) *DatastoreBackend {
	return &DatastoreBackend{ds: ds}
}

// ReadTuple implements Backend.
func (b *DatastoreBackend) ReadTuple(ctx context.Context, store string, key *openfgav1.TupleKeyWithoutCondition) (*openfgav1.Tuple, error) {
	t, err := b.ds.ReadUserTuple(ctx, store, storage.ReadUserTupleFilter{
		Object:   key.GetObject(),
		Relation: key.GetRelation(),
		User:     key.GetUser(),
	}, storage.ReadUserTupleOptions{})
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s", ErrTupleNotFound, tupleKeyStringFrom(key))
		}
		return nil, err
	}
	return t, nil
}

// ReadByUser implements Backend.
func (b *DatastoreBackend) ReadByUser(ctx context.Context, store, user string, fn func(*openfgav1.Tuple) error) error {
	if user == "" {
		return errors.New("internal invariant: ReadByUser requires a non-empty user")
	}
	iter, err := b.ds.Read(ctx, store, storage.ReadFilter{User: user}, storage.ReadOptions{})
	if err != nil {
		return err
	}
	defer iter.Stop()
	return iterate(ctx, iter, fn)
}

// ReadObject implements Backend.
func (b *DatastoreBackend) ReadObject(ctx context.Context, store, object string, fn func(*openfgav1.Tuple) error) error {
	if object == "" {
		return errors.New("internal invariant: ReadObject requires a non-empty object")
	}
	iter, err := b.ds.Read(ctx, store, storage.ReadFilter{Object: object}, storage.ReadOptions{})
	if err != nil {
		return err
	}
	defer iter.Stop()
	return iterate(ctx, iter, fn)
}

// WriteBatch implements Backend.
func (b *DatastoreBackend) WriteBatch(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes, idempotent bool) error {
	opts := []storage.TupleWriteOption(nil)
	if idempotent {
		opts = append(opts,
			storage.WithOnMissingDelete(storage.OnMissingDeleteIgnore),
			storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore),
		)
	}
	return b.ds.Write(ctx, store, deletes, writes, opts...)
}

// MaxBatchSize implements Backend.
func (b *DatastoreBackend) MaxBatchSize() int { return b.ds.MaxTuplesPerWrite() }

// GetAuthorizationModel implements Backend.
func (b *DatastoreBackend) GetAuthorizationModel(ctx context.Context, store, modelID string) (*openfgav1.AuthorizationModel, error) {
	if modelID == "" {
		return b.ds.FindLatestAuthorizationModel(ctx, store)
	}
	return b.ds.ReadAuthorizationModel(ctx, store, modelID)
}

// FindStoreByName implements Backend.
func (b *DatastoreBackend) FindStoreByName(ctx context.Context, name string) (*openfgav1.Store, error) {
	const pageSize int32 = 100
	var from string
	var matches []*openfgav1.Store
	for {
		page, cont, err := b.ds.ListStores(ctx, storage.ListStoresOptions{
			Name:       name,
			Pagination: storage.NewPaginationOptions(pageSize, from),
		})
		if err != nil {
			return nil, err
		}
		matches = append(matches, page...)
		if cont == "" {
			break
		}
		from = cont
	}
	switch len(matches) {
	case 0:
		return nil, nil
	case 1:
		return matches[0], nil
	default:
		return nil, fmt.Errorf("%w: %d stores are named %q; pass an explicit control store id to disambiguate",
			ErrAmbiguousControlStore, len(matches), name)
	}
}

// CreateStoreWithModel implements Backend.
func (b *DatastoreBackend) CreateStoreWithModel(ctx context.Context, name string, model *openfgav1.AuthorizationModel, extraWrites []*openfgav1.TupleKey) (string, string, error) {
	storeID := ulid.Make().String()
	if _, err := b.ds.CreateStore(ctx, &openfgav1.Store{Id: storeID, Name: name}); err != nil {
		return "", "", err
	}

	provisioned := proto.Clone(model).(*openfgav1.AuthorizationModel)
	provisioned.Id = ulid.Make().String()
	if err := b.ds.WriteAuthorizationModel(ctx, storeID, provisioned); err != nil {
		return "", "", err
	}

	if len(extraWrites) > 0 {
		if err := b.ds.Write(ctx, storeID, nil, extraWrites); err != nil {
			return "", "", err
		}
	}
	return storeID, provisioned.GetId(), nil
}

// ProvisionModelOnStore implements Backend.
func (b *DatastoreBackend) ProvisionModelOnStore(ctx context.Context, storeID string, model *openfgav1.AuthorizationModel, extraWrites []*openfgav1.TupleKey) (string, error) {
	provisioned := proto.Clone(model).(*openfgav1.AuthorizationModel)
	provisioned.Id = ulid.Make().String()
	if err := b.ds.WriteAuthorizationModel(ctx, storeID, provisioned); err != nil {
		return "", err
	}
	if len(extraWrites) > 0 {
		if err := b.ds.Write(ctx, storeID, nil, extraWrites); err != nil {
			return "", err
		}
	}
	return provisioned.GetId(), nil
}

func iterate(ctx context.Context, iter storage.TupleIterator, fn func(*openfgav1.Tuple) error) error {
	for {
		t, err := iter.Next(ctx)
		if errors.Is(err, storage.ErrIteratorDone) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(t); err != nil {
			return err
		}
	}
}

func tupleKeyStringFrom(key *openfgav1.TupleKeyWithoutCondition) string {
	return key.GetObject() + "#" + key.GetRelation() + "@" + key.GetUser()
}
