package tuplesync

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"google.golang.org/protobuf/types/known/structpb"
)

// MySQL is the most restrictive supported backend (see
// assets/migrations/mysql/001_initialize_schema.sql and 006). All ledger
// tuples must fit these envelopes so the control store works on every backend.
const (
	mysqlMaxObjectTypeLength = 128
	mysqlMaxObjectIDLength   = 255
	mysqlMaxRelationLength   = 50
	mysqlMaxUserLength       = 256

	// maxSourceIDLength bounds the roster source id so that
	// "fga_source:<source>" fits the user column.
	maxSourceIDLength = mysqlMaxUserLength - len(controlSourceType) - 1

	// payloadChunkRawLimit keeps each fga_pc chunk id inside all column
	// envelopes even when claim hash + index + base64 overhead are added.
	payloadChunkRawLimit = 130
)

// ErrLedgerUnverifiable wraps every condition under which the ownership
// ledger cannot be trusted (corrupt chunks, conflicting owners, model drift,
// digest mismatch). Use errors.Is to detect it.
var ErrLedgerUnverifiable = errors.New("ownership ledger is unverifiable")

func ledgerError(claimObject, detail string, args ...any) error {
	return fmt.Errorf("%w: claim %s: %s", ErrLedgerUnverifiable, claimObject, fmt.Sprintf(detail, args...))
}

// Claim is one ownership record: Source manages one tuple (including its
// condition) in target StoreID.
type Claim struct {
	StoreID string
	Source  string
	key     CanonicalKey
}

// newClaim builds a Claim and pre-computes its deterministic digest.
func newClaim(storeID, source string, tk *openfgav1.TupleKey) (*Claim, error) {
	k, err := newCanonicalKey(storeID, tk)
	if err != nil {
		return nil, err
	}
	return &Claim{StoreID: storeID, Source: source, key: k}, nil
}

// hash is the deterministic claim id derived from (store, canonical tuple).
// It deliberately excludes the source, so two sources attempting to claim the
// same tuple collide on the same object and the conflict is detectable.
func (c *Claim) hash() string { return c.key.Hash() }

// object is the claim object in the control store.
func (c *Claim) object() string { return controlClaimType + ":" + c.key.Hash() }

// tupleKey reconstructs the claimed target tuple.
func (c *Claim) tupleKey() (*openfgav1.TupleKey, error) { return c.key.TupleKey() }

// encodeTuples renders the claim as deterministic ledger tuples:
// one owner, one target, and one or more payload chunks. The returned slice
// is sorted for stable batching and diffing.
func (c *Claim) encodeTuples() []*openfgav1.TupleKey {
	obj := c.object()
	keys := []*openfgav1.TupleKey{
		{Object: obj, Relation: relOwner, User: controlSourceType + ":" + c.Source},
		{Object: obj, Relation: relTarget, User: controlStoreType + ":" + c.StoreID},
	}

	payload := c.key.Payload()
	for idx, start := 0, 0; start < len(payload); idx, start = idx+1, start+payloadChunkRawLimit {
		end := min(start+payloadChunkRawLimit, len(payload))
		chunk := base64.RawURLEncoding.EncodeToString([]byte(payload[start:end]))
		keys = append(keys, &openfgav1.TupleKey{
			Object:   obj,
			Relation: relPart,
			User:     formatPartUser(c.hash(), idx, chunk),
		})
	}

	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Relation != keys[j].Relation {
			return keys[i].Relation < keys[j].Relation
		}
		return keys[i].User < keys[j].User
	})
	return keys
}

// encodeDeletes renders the same ledger tuples as delete keys (condition is
// not part of tuple identity).
func (c *Claim) encodeDeletes() []*openfgav1.TupleKeyWithoutCondition {
	keys := c.encodeTuples()
	out := make([]*openfgav1.TupleKeyWithoutCondition, 0, len(keys))
	for _, k := range keys {
		out = append(out, tupleUtils.TupleKeyToTupleKeyWithoutCondition(k))
	}
	return out
}

// formatPartUser builds "fga_pc:<hash>-<index>-<base64 chunk>".
func formatPartUser(hash string, index int, encodedChunk string) string {
	return fmt.Sprintf("%s:%s-%d-%s", controlPartType, hash, index, encodedChunk)
}

// parsePartUser reverses formatPartUser, returning the index and the
// base64-encoded chunk.
func parsePartUser(user string) (index int, encodedChunk string, ok bool) {
	id, found := strings.CutPrefix(user, controlPartType+":")
	if !found {
		return 0, "", false
	}
	rest := strings.SplitN(id, "-", 3)
	if len(rest) != 3 {
		return 0, "", false
	}
	idx, err := strconv.Atoi(rest[1])
	if err != nil || idx < 0 {
		return 0, "", false
	}
	return idx, rest[2], true
}

// partHash extracts the claim hash embedded in a part chunk user.
func partHash(user string) (string, bool) {
	id, found := strings.CutPrefix(user, controlPartType+":")
	if !found {
		return "", false
	}
	rest := strings.SplitN(id, "-", 3)
	if len(rest) != 3 {
		return "", false
	}
	return rest[0], true
}

// decodeClaimObject reconstructs a Claim from all ledger tuples stored on one
// fga_claim object. Any structural problem, tampering or digest mismatch is
// reported as ErrLedgerUnverifiable carrying the claim object id for
// operator diagnostics.
func decodeClaimObject(claimObject string, tuples []*openfgav1.Tuple) (*Claim, error) {
	hash, found := strings.CutPrefix(claimObject, controlClaimType+":")
	if !found || len(hash) == 0 {
		return nil, fmt.Errorf("%w: %q is not a claim object", ErrLedgerUnverifiable, claimObject)
	}

	var owners, targets []string
	parts := make(map[int]string)

	for _, tp := range tuples {
		k := tp.GetKey()
		if k.GetObject() != claimObject {
			return nil, ledgerError(claimObject, "unexpected tuple on foreign object %q", k.GetObject())
		}
		switch k.GetRelation() {
		case relOwner:
			id, ok := cutTypeUser(k.GetUser(), controlSourceType)
			if !ok {
				return nil, ledgerError(claimObject, "owner has malformed user %q", k.GetUser())
			}
			owners = append(owners, id)
		case relTarget:
			id, ok := cutTypeUser(k.GetUser(), controlStoreType)
			if !ok {
				return nil, ledgerError(claimObject, "target has malformed user %q", k.GetUser())
			}
			targets = append(targets, id)
		case relPart:
			idx, chunk, ok := parsePartUser(k.GetUser())
			if !ok {
				return nil, ledgerError(claimObject, "part has malformed user %q", k.GetUser())
			}
			embeddedHash, ok := partHash(k.GetUser())
			if !ok || embeddedHash != hash {
				return nil, ledgerError(claimObject, "part chunk %d belongs to claim %s, not this claim", idx, embeddedHash)
			}
			if _, dup := parts[idx]; dup {
				return nil, ledgerError(claimObject, "duplicate payload chunk index %d", idx)
			}
			parts[idx] = chunk
		default:
			return nil, ledgerError(claimObject, "unknown relation %q", k.GetRelation())
		}
	}

	if len(owners) == 0 || len(targets) == 0 || len(parts) == 0 {
		return nil, ledgerError(claimObject,
			"incomplete claim: %d owner(s), %d target(s), %d part(s)", len(owners), len(targets), len(parts))
	}
	if len(distinct(owners)) != 1 {
		return nil, ledgerError(claimObject, "multiple conflicting owners %v", distinct(owners))
	}
	if len(distinct(targets)) != 1 {
		return nil, ledgerError(claimObject, "multiple conflicting targets %v", distinct(targets))
	}
	for idx := 0; idx < len(parts); idx++ {
		if _, ok := parts[idx]; !ok {
			return nil, ledgerError(claimObject, "payload chunk index %d is missing", idx)
		}
	}

	var payload strings.Builder
	for idx := 0; idx < len(parts); idx++ {
		raw, err := base64.RawURLEncoding.DecodeString(parts[idx])
		if err != nil {
			return nil, ledgerError(claimObject, "payload chunk %d is not valid base64url: %v", idx, err)
		}
		payload.Write(raw)
	}

	var p tuplePayload
	if err := json.Unmarshal([]byte(payload.String()), &p); err != nil {
		return nil, ledgerError(claimObject, "payload is not valid JSON: %v", err)
	}

	storeID := targets[0]
	tk, err := payloadToTupleKey(&p)
	if err != nil {
		return nil, ledgerError(claimObject, "payload cannot be converted: %v", err)
	}
	ck, err := newCanonicalKey(storeID, tk)
	if err != nil {
		return nil, ledgerError(claimObject, "payload cannot be canonicalized: %v", err)
	}
	if ck.Hash() != hash {
		return nil, ledgerError(claimObject,
			"digest mismatch: ledger object claims %s but payload recomputes %s; refusing to guess ownership",
			hash, ck.Hash())
	}

	return &Claim{StoreID: storeID, Source: owners[0], key: ck}, nil
}

func payloadToTupleKey(p *tuplePayload) (*openfgav1.TupleKey, error) {
	tk := &openfgav1.TupleKey{Object: p.Object, Relation: p.Relation, User: p.User}
	if p.Condition != nil {
		tk.Condition = &openfgav1.RelationshipCondition{Name: p.Condition.Name}
		if len(p.Condition.Context) > 0 {
			var m map[string]any
			if err := json.Unmarshal(p.Condition.Context, &m); err != nil {
				return nil, err
			}
			s, err := structpb.NewStruct(m)
			if err != nil {
				return nil, err
			}
			tk.Condition.Context = s
		}
	}
	return tk, nil
}

func cutTypeUser(user, typ string) (string, bool) {
	id, found := strings.CutPrefix(user, typ+":")
	if !found || id == "" || strings.ContainsAny(id, "#: \x00") {
		return "", false
	}
	return id, true
}

func distinct(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := in[:0]
	for _, v := range in {
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}
