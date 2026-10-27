package tuplesync

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

// DefaultControlStoreName is the conventional name used for the dedicated
// control store when no explicit id or name is supplied.
const DefaultControlStoreName = "fga-tuplesync-control"

// ErrAmbiguousControlStore means more than one store matches the configured
// control-store name.
var ErrAmbiguousControlStore = errors.New("ambiguous control store")

// ErrReservedTypeCollision means the target business model declares a type
// whose name is reserved for tuplesync's control model.
var ErrReservedTypeCollision = errors.New("control model reserved type collision")

// EnsureControlStore finds (or bootstraps) the dedicated control store and
// validates that its model matches this binary's control model and codec
// version. It is idempotent: a correctly provisioned store is never written
// to. Model drift, codec mismatch or a failed earlier bootstrap (store
// without any model) are handled explicitly rather than silently "repaired"
// into an unknown state, except for the well-defined interrupted-creation
// case (store exists but has no model at all), which is completed.
func EnsureControlStore(ctx context.Context, b Backend, name string, targetModel *openfgav1.AuthorizationModel) (string, error) {
	if err := checkReservedTypeCollision(targetModel); err != nil {
		return "", err
	}

	store, err := b.FindStoreByName(ctx, name)
	if err != nil {
		return "", err
	}

	if store == nil {
		storeID, _, err := b.CreateStoreWithModel(ctx, name, ControlModel(),
			[]*openfgav1.TupleKey{versionMarkerTuple()})
		if err != nil {
			return "", fmt.Errorf("bootstrap control store: %w", err)
		}
		return storeID, nil
	}

	storeID := store.GetId()
	model, err := b.GetAuthorizationModel(ctx, storeID, "")
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Interrupted bootstrap: the store was created but the model
			// never made it. Completing it is the unique safe recovery,
			// because without a model no ledger tuples could have been
			// written (writes validate against it).
			if _, err := b.ProvisionModelOnStore(ctx, storeID, ControlModel(),
				[]*openfgav1.TupleKey{versionMarkerTuple()}); err != nil {
				return "", fmt.Errorf("complete interrupted bootstrap for store %s: %w", storeID, err)
			}
			return storeID, nil
		}
		return "", err
	}

	if err := verifyControlModel(model); err != nil {
		return "", err
	}
	if err := verifyVersionMarker(ctx, b, storeID); err != nil {
		return "", err
	}
	return storeID, nil
}

func checkReservedTypeCollision(targetModel *openfgav1.AuthorizationModel) error {
	if targetModel == nil {
		return nil
	}
	for _, td := range targetModel.GetTypeDefinitions() {
		for _, reserved := range ReservedTypeNames() {
			if td.GetType() == reserved {
				return fmt.Errorf("%w: target model declares %q, which is reserved for tuplesync ledger metadata; rename the business type",
					ErrReservedTypeCollision, reserved)
			}
		}
	}
	return nil
}

func verifyControlModel(model *openfgav1.AuthorizationModel) error {
	expected := ControlModel()
	if model.GetSchemaVersion() != expected.GetSchemaVersion() {
		return fmt.Errorf("%w: control store model schema is %q, expected %q",
			ErrLedgerUnverifiable, model.GetSchemaVersion(), expected.GetSchemaVersion())
	}

	gotTS, err := typesystem.New(model)
	if err != nil {
		return fmt.Errorf("%w: control store model cannot be loaded: %v", ErrLedgerUnverifiable, err)
	}
	expTS, err := typesystem.New(expected)
	if err != nil {
		return err // cannot happen: the embedded model is a compile-time constant
	}

	if len(model.GetTypeDefinitions()) != len(expected.GetTypeDefinitions()) {
		return fmt.Errorf("%w: control store model defines %d types, expected %d",
			ErrLedgerUnverifiable, len(model.GetTypeDefinitions()), len(expected.GetTypeDefinitions()))
	}

	for _, expTD := range expected.GetTypeDefinitions() {
		gotTD, ok := gotTS.GetTypeDefinition(expTD.GetType())
		if !ok {
			return fmt.Errorf("%w: control store model is missing reserved type %q", ErrLedgerUnverifiable, expTD.GetType())
		}
		if !sameRelationSets(gotTD.GetRelations(), expTD.GetRelations()) {
			return fmt.Errorf("%w: control store type %q defines relations %v, expected %v",
				ErrLedgerUnverifiable, expTD.GetType(),
				relationNames(gotTD.GetRelations()), relationNames(expTD.GetRelations()))
		}
	}

	_ = expTS
	return nil
}

func sameRelationSets(a, b map[string]*openfgav1.Userset) bool {
	if len(a) != len(b) {
		return false
	}
	for name := range b {
		if _, ok := a[name]; !ok {
			return false
		}
	}
	return true
}

func verifyVersionMarker(ctx context.Context, b Backend, controlStoreID string) error {
	var versions []string
	err := b.ReadObject(ctx, controlStoreID, sysControlObject, func(t *openfgav1.Tuple) error {
		k := t.GetKey()
		if k.GetRelation() == relVersion {
			versions = append(versions, k.GetUser())
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(versions) != 1 || versions[0] != controlSourceType+":"+CodecVersion {
		return fmt.Errorf("%w: control store codec version marker is %v, expected [%s:%s]",
			ErrLedgerUnverifiable, versions, controlSourceType, CodecVersion)
	}
	return nil
}
