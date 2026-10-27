package tuplesync

import (
	"sort"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
)

// CodecVersion identifies the on-the-wire shape of the control store model
// and the ledger claim encoding. It is recorded in the control store at
// bootstrap (see versionMarkerTuple) and must be bumped on any incompatible
// change.
const CodecVersion = "v1"

// Reserved names of the fixed control-store model.
const (
	controlSourceType = "fga_source" // roster source ids
	controlStoreType  = "fga_store"  // target store ids
	controlPartType   = "fga_pc"     // payload chunk objects
	controlRunType    = "fga_run"    // sync run (lease holder) ulids
	controlSysType    = "fga_sys"    // control-store metadata objects
	controlLeaseType  = "fga_lease"  // one object per (store, source) lease
	controlClaimType  = "fga_claim"  // one object per claimed tuple

	relOwner   = "owner"   // fga_claim#owner  @ fga_source:<id>
	relTarget  = "target"  // fga_claim#target @ fga_store:<id>
	relPart    = "part"    // fga_claim#part   @ fga_pc:<hash>-<idx>-<chunk>
	relHolder  = "holder"  // fga_lease#holder @ fga_run:<ulid>
	relVersion = "version" // fga_sys:control#version @ fga_source:<CodecVersion>

	sysControlObject = controlSysType + ":control"
)

// controlModelDSL is the immutable model of the dedicated control store.
// Every tuple written by the syncer into the control store must validate
// against this model.
const controlModelDSL = `
model
  schema 1.1

type ` + controlSourceType + `
type ` + controlStoreType + `
type ` + controlPartType + `
type ` + controlRunType + `

type ` + controlSysType + `
  relations
    define ` + relVersion + `: [` + controlSourceType + `]

type ` + controlLeaseType + `
  relations
    define ` + relHolder + `: [` + controlRunType + `]

type ` + controlClaimType + `
  relations
    define ` + relOwner + `: [` + controlSourceType + `]
    define ` + relTarget + `: [` + controlStoreType + `]
    define ` + relPart + `: [` + controlPartType + `]
`

// ControlModel returns the fixed authorization model provisioned in the
// control store. Callers must not mutate the returned proto.
func ControlModel() *openfgav1.AuthorizationModel {
	return parser.MustTransformDSLToProto(controlModelDSL)
}

// ReservedTypeNames lists all types owned by the control-store model. The
// bootstrap refuses to run when the target business model declares any of
// them, to avoid silent collisions.
func ReservedTypeNames() []string {
	return []string{
		controlClaimType,
		controlLeaseType,
		controlPartType,
		controlRunType,
		controlSourceType,
		controlStoreType,
		controlSysType,
	}
}

// relationNames returns the sorted relation names declared on a type.
func relationNames(rels map[string]*openfgav1.Userset) []string {
	out := make([]string, 0, len(rels))
	for k := range rels {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// versionMarkerTuple is written exactly once at bootstrap and read back to
// confirm the control store matches this binary's CodecVersion.
func versionMarkerTuple() *openfgav1.TupleKey {
	return &openfgav1.TupleKey{
		Object:   sysControlObject,
		Relation: relVersion,
		User:     controlSourceType + ":" + CodecVersion,
	}
}
