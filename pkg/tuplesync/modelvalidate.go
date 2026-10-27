package tuplesync

import (
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/validation"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// validateTupleForModel applies the same tuple acceptance rules as the Write
// API command (see pkg/server/commands/write.go): model/type-system
// validation plus the implicit-tuple rejection. It intentionally does not
// enforce the condition context byte limit here; callers that target the
// Write API should pass the configured limit to validateConditionContextSize.
func validateTupleForModel(ts *typesystem.TypeSystem, tk *openfgav1.TupleKey) error {
	if err := validation.ValidateTupleForWrite(ts, tk); err != nil {
		return err
	}
	userObject, userRelation := tupleUtils.SplitObjectRelation(tk.GetUser())
	if tk.GetRelation() == userRelation && tk.GetObject() == userObject {
		return fmt.Errorf("cannot write tuple %s: it is implicit", tupleUtils.TupleKeyToString(tk))
	}
	return nil
}
