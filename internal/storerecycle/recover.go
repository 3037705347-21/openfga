package storerecycle

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/oklog/ulid/v2"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storerecycle"
)

// Recover runs the full staged recovery pipeline for a recycled store:
//
//	COPY     -> snapshot copied into an isolated staging datastore
//	VERIFY   -> staged content re-read and checked against the manifest
//	TAKEOVER -> verified snapshot atomically installed under the original id
//
// Copy/verify failures mark the session FAILED, discard the staging
// datastore and leave the original store id invisible; a later call starts a
// brand-new attempt. A context cancellation is treated like a crash: the
// session stays non-terminal and the staging copy is abandoned, so the
// startup sweep can reconcile it.
func (s *Service) Recover(ctx context.Context, storeID string) (*storerecycle.RecoverySession, error) {
	now := s.clock()
	session := &storerecycle.RecoverySession{
		ID:             ulid.Make().String(),
		StagingStoreID: ulid.Make().String(),
		Status:         storerecycle.RecoveryRunning,
		StartedAt:      now,
		HeartbeatAt:    now,
		UpdatedAt:      now,
		Progress:       storerecycle.Progress{Phase: storerecycle.PhaseCopy},
	}

	session, err := s.backend.InitRecovery(ctx, storeID, session)
	if err != nil {
		return nil, fmt.Errorf("recover store %s: %w", storeID, err)
	}
	sessionID := session.ID
	stagingID := session.StagingStoreID

	snapshot, err := s.backend.GetSnapshot(ctx, storeID)
	if err != nil {
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("load snapshot: %w", err))
	}
	session.Progress = initialProgress(snapshot.Manifest)
	if err := s.persistProgress(ctx, storeID, sessionID, session.Progress); err != nil {
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("persist progress: %w", err))
	}

	staging, err := s.newStaging()
	if err != nil {
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("create staging: %w", err))
	}
	s.registerStaging(sessionID, staging)

	if _, err := staging.CreateStore(ctx, &openfgav1.Store{
		Id:        stagingID,
		Name:      "restore-staging-" + storeID,
		CreatedAt: timestamppb.New(now),
		UpdatedAt: timestamppb.New(now),
	}); err != nil {
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("create staging store: %w", err))
	}

	if err := s.copyToStaging(ctx, storeID, sessionID, stagingID, staging, snapshot); err != nil {
		if isContextDone(err) {
			s.abandonStaging(sessionID)
			return nil, err
		}
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("copy: %w", err))
	}

	verifiedManifest, err := s.verifyStaging(ctx, storeID, sessionID, stagingID, staging, snapshot)
	if err != nil {
		if isContextDone(err) {
			s.abandonStaging(sessionID)
			return nil, err
		}
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("verify: %w", err))
	}

	takeoverStarted := s.clock()
	if err := s.takeover(ctx, storeID, sessionID, snapshot); err != nil {
		if isContextDone(err) {
			s.abandonStaging(sessionID)
			return nil, err
		}
		s.appendTakeoverRecord(ctx, storeID, sessionID, stagingID, snapshot.Manifest, takeoverStarted, storerecycle.TakeoverFailed, err.Error())
		return nil, s.failRecovery(ctx, storeID, sessionID, fmt.Errorf("takeover: %w", err))
	}
	s.appendTakeoverRecord(ctx, storeID, sessionID, stagingID, verifiedManifest, takeoverStarted, storerecycle.TakeoverSucceeded, "")

	completed, err := s.backend.CompleteRecovery(ctx, storeID, sessionID, s.clock())
	if err != nil {
		// Content is already live; surface the bookkeeping error without
		// discarding the installed data.
		return nil, fmt.Errorf("recover store %s: complete recovery: %w", storeID, err)
	}
	s.abandonStaging(sessionID)

	s.logger.Info("store recovered from recycle bin",
		zap.String("store_id", storeID),
		zap.String("session_id", sessionID),
		zap.Int("models", snapshot.Manifest.ModelsCount),
		zap.Int("tuples", snapshot.Manifest.TuplesCount))
	return completed, nil
}

func initialProgress(m storerecycle.Manifest) storerecycle.Progress {
	return storerecycle.Progress{
		Phase:                storerecycle.PhaseCopy,
		ModelsTotal:          m.ModelsCount,
		TuplesTotal:          m.TuplesCount,
		AssertionModelsTotal: m.AssertionModels,
		AssertionsTotal:      m.AssertionsCount,
	}
}

func (s *Service) persistProgress(
	ctx context.Context,
	storeID, sessionID string,
	p storerecycle.Progress,
) error {
	progress := p
	_, err := s.backend.UpdateRecovery(ctx, storeID, sessionID, func(sess *storerecycle.RecoverySession) error {
		now := s.clock()
		sess.Progress = progress
		sess.HeartbeatAt = now
		sess.UpdatedAt = now
		return nil
	})
	if err == nil && s.observe != nil {
		s.observe(progress)
	}
	return err
}

// copyToStaging copies the snapshot into the staging datastore in batches and
// persists progress after every batch. All copied protobuf values are
// independent clones.
func (s *Service) copyToStaging(
	ctx context.Context,
	storeID, sessionID, stagingID string,
	staging storage.OpenFGADatastore,
	snapshot *storerecycle.Snapshot,
) error {
	p := initialProgress(snapshot.Manifest)

	// Models: page-sized batches.
	for start := 0; start < len(snapshot.Models); start += s.modelPageSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+s.modelPageSize, len(snapshot.Models))
		for _, m := range snapshot.Models[start:end] {
			if err := staging.WriteAuthorizationModel(ctx, stagingID, proto.Clone(m).(*openfgav1.AuthorizationModel)); err != nil {
				return fmt.Errorf("write model %s: %w", m.GetId(), err)
			}
			p.ModelsCopied++
		}
		if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
			return err
		}
	}

	// Tuples: honor the staging datastore write batching limit.
	maxBatch := staging.MaxTuplesPerWrite()
	if maxBatch <= 0 {
		maxBatch = len(snapshot.Tuples)
	}
	for start := 0; start < len(snapshot.Tuples); start += maxBatch {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := min(start+maxBatch, len(snapshot.Tuples))
		batch := make([]*openfgav1.TupleKey, 0, end-start)
		for _, tk := range snapshot.Tuples[start:end] {
			batch = append(batch, proto.Clone(tk).(*openfgav1.TupleKey))
		}
		if err := staging.Write(ctx, stagingID, nil, batch); err != nil {
			return fmt.Errorf("write tuples [%d:%d]: %w", start, end, err)
		}
		p.TuplesCopied += end - start
		if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
			return err
		}
	}

	// Assertions: one group per model. Iterate model ids in deterministic
	// order so progress observations are reproducible.
	modelIDs := make([]string, 0, len(snapshot.Assertions))
	for id := range snapshot.Assertions {
		modelIDs = append(modelIDs, id)
	}
	sort.Strings(modelIDs)
	for _, modelID := range modelIDs {
		asserts := snapshot.Assertions[modelID]
		if err := ctx.Err(); err != nil {
			return err
		}
		cloned := make([]*openfgav1.Assertion, 0, len(asserts))
		for _, a := range asserts {
			cloned = append(cloned, proto.Clone(a).(*openfgav1.Assertion))
		}
		if err := staging.WriteAssertions(ctx, stagingID, modelID, cloned); err != nil {
			return fmt.Errorf("write assertions for model %s: %w", modelID, err)
		}
		p.AssertionModelsCopied++
		p.AssertionsCopied += len(asserts)
		if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
			return err
		}
	}

	return nil
}

// verifyStaging enumerates the staging datastore and compares counts,
// fingerprints and the latest model id against the captured manifest.
func (s *Service) verifyStaging(
	ctx context.Context,
	storeID, sessionID, stagingID string,
	staging storage.OpenFGADatastore,
	snapshot *storerecycle.Snapshot,
) (storerecycle.Manifest, error) {
	sess, err := s.backend.GetRecovery(ctx, storeID, sessionID)
	if err != nil {
		return storerecycle.Manifest{}, err
	}
	p := sess.Progress
	p.Phase = storerecycle.PhaseVerify
	if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
		return storerecycle.Manifest{}, err
	}

	staged, err := enumerateSnapshot(ctx, staging, &openfgav1.Store{Id: stagingID}, snapshot.Manifest.CapturedAt, s.modelPageSize)
	if err != nil {
		return storerecycle.Manifest{}, err
	}

	want := snapshot.Manifest
	got := staged.Manifest
	var mismatches []storerecycle.VerificationMismatch
	add := func(kind, property string, expected, actual any) {
		mismatches = append(mismatches, storerecycle.VerificationMismatch{
			Kind: kind, Property: property,
			Expected: fmt.Sprint(expected), Actual: fmt.Sprint(actual),
		})
	}

	if got.ModelsCount != want.ModelsCount {
		add("models", "count", want.ModelsCount, got.ModelsCount)
	}
	if got.ModelsFingerprint != want.ModelsFingerprint {
		add("models", "fingerprint", want.ModelsFingerprint, got.ModelsFingerprint)
	}
	if got.LatestModelID != want.LatestModelID {
		add("models", "latest_model", want.LatestModelID, got.LatestModelID)
	}
	if got.TuplesCount != want.TuplesCount {
		add("tuples", "count", want.TuplesCount, got.TuplesCount)
	}
	if got.TuplesFingerprint != want.TuplesFingerprint {
		add("tuples", "fingerprint", want.TuplesFingerprint, got.TuplesFingerprint)
	}
	if got.AssertionModels != want.AssertionModels {
		add("assertions", "model_count", want.AssertionModels, got.AssertionModels)
	}
	if got.AssertionsCount != want.AssertionsCount {
		add("assertions", "count", want.AssertionsCount, got.AssertionsCount)
	}
	if got.AssertionsFingerprint != want.AssertionsFingerprint {
		add("assertions", "fingerprint", want.AssertionsFingerprint, got.AssertionsFingerprint)
	}
	if len(mismatches) > 0 {
		return storerecycle.Manifest{}, &storerecycle.VerificationMismatchError{Mismatches: mismatches}
	}

	p.ModelsVerified = got.ModelsCount
	p.TuplesVerified = got.TuplesCount
	p.AssertionModelsVerified = got.AssertionModels
	p.AssertionsVerified = got.AssertionsCount
	if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
		return storerecycle.Manifest{}, err
	}
	return got, nil
}

// takeover installs the verified snapshot into the live datastore under the
// original store id. Datastores implementing [storerecycle.StoreInstaller]
// get a single-critical-section publish; everything else uses the degraded
// "write all data, create the store header last" ordering, under which the
// store id stays invisible (GetStore returns NotFound) until every row is in
// place.
func (s *Service) takeover(
	ctx context.Context,
	storeID, sessionID string,
	snapshot *storerecycle.Snapshot,
) error {
	sess, err := s.backend.GetRecovery(ctx, storeID, sessionID)
	if err != nil {
		return err
	}
	p := sess.Progress
	p.Phase = storerecycle.PhaseTakeover
	if err := s.persistProgress(ctx, storeID, sessionID, p); err != nil {
		return err
	}

	header := proto.Clone(snapshot.Store).(*openfgav1.Store)
	header.UpdatedAt = timestamppb.New(s.clock())

	payload := storerecycle.InstallPayload{
		Store:      header,
		Models:     snapshot.Models,
		Tuples:     snapshot.Tuples,
		Assertions: snapshot.Assertions,
	}

	if installer, ok := s.live.(storerecycle.StoreInstaller); ok {
		if err := installer.InstallStore(ctx, payload); err != nil {
			if errors.Is(err, storage.ErrCollision) {
				return fmt.Errorf("%w: %s", storerecycle.ErrStoreAlreadyActive, storeID)
			}
			return err
		}
	} else {
		if err := s.fallbackInstall(ctx, payload); err != nil {
			return err
		}
	}

	// Read-back: the original id must resolve immediately after the switch.
	store, err := s.live.GetStore(ctx, storeID)
	if err != nil {
		return fmt.Errorf("read back restored store: %w", err)
	}
	if store.GetId() != storeID {
		return fmt.Errorf("restored store id mismatch: got %s", store.GetId())
	}
	return nil
}

// fallbackInstall publishes the content through the standard datastore
// interfaces. The store header is created only after every data row exists,
// so data-plane operations that resolve the store keep seeing NotFound until
// the publish is complete. It is best-effort for backends lacking the atomic
// capability; retries are safe on the in-memory backend because identical
// model/tuple/assertion writes are idempotent there.
func (s *Service) fallbackInstall(ctx context.Context, payload storerecycle.InstallPayload) error {
	if _, err := s.live.GetStore(ctx, payload.Store.GetId()); err == nil {
		return fmt.Errorf("%w: %s", storerecycle.ErrStoreAlreadyActive, payload.Store.GetId())
	}

	for _, m := range payload.Models {
		if err := s.live.WriteAuthorizationModel(ctx, payload.Store.GetId(), m); err != nil {
			return fmt.Errorf("write model %s: %w", m.GetId(), err)
		}
	}

	maxBatch := s.live.MaxTuplesPerWrite()
	if maxBatch <= 0 {
		maxBatch = len(payload.Tuples)
	}
	for start := 0; start < len(payload.Tuples); start += maxBatch {
		end := min(start+maxBatch, len(payload.Tuples))
		if err := s.live.Write(ctx, payload.Store.GetId(), nil, payload.Tuples[start:end]); err != nil {
			return fmt.Errorf("write tuples [%d:%d]: %w", start, end, err)
		}
	}

	for modelID, asserts := range payload.Assertions {
		if err := s.live.WriteAssertions(ctx, payload.Store.GetId(), modelID, asserts); err != nil {
			return fmt.Errorf("write assertions for model %s: %w", modelID, err)
		}
	}

	// Visibility flips only here.
	if _, err := s.live.CreateStore(ctx, payload.Store); err != nil {
		if errors.Is(err, storage.ErrCollision) {
			return fmt.Errorf("%w: %s", storerecycle.ErrStoreAlreadyActive, payload.Store.GetId())
		}
		return fmt.Errorf("create restored store header: %w", err)
	}
	return nil
}

// failRecovery marks the session FAILED, returns the entry to RECYCLED,
// discards the staging datastore and returns an error wrapping the cause so
// callers can still match sentinels such as ErrVerificationFailed.
func (s *Service) failRecovery(
	ctx context.Context,
	storeID, sessionID string,
	cause error,
) error {
	s.abandonStaging(sessionID)
	if _, err := s.backend.FailRecovery(ctx, storeID, sessionID, cause.Error(), s.clock()); err != nil {
		s.logger.Error("failed to mark recovery session FAILED",
			zap.String("store_id", storeID), zap.String("session_id", sessionID), zap.Error(err))
	}
	return fmt.Errorf("recover store %s: %w", storeID, cause)
}

func (s *Service) appendTakeoverRecord(
	ctx context.Context,
	storeID, sessionID, stagingID string,
	manifest storerecycle.Manifest,
	startedAt time.Time,
	result storerecycle.TakeoverResult,
	failureReason string,
) {
	record := &storerecycle.TakeoverRecord{
		ID:             ulid.Make().String(),
		StoreID:        storeID,
		SessionID:      sessionID,
		StagingStoreID: stagingID,
		Result:         result,
		Manifest:       manifest,
		Error:          failureReason,
		StartedAt:      startedAt,
		CompletedAt:    s.clock(),
	}
	if err := s.backend.AppendTakeover(ctx, record); err != nil {
		s.logger.Error("failed to append takeover record",
			zap.String("store_id", storeID), zap.String("session_id", sessionID), zap.Error(err))
	}
}

func (s *Service) registerStaging(sessionID string, ds storage.OpenFGADatastore) {
	s.stagingsMu.Lock()
	defer s.stagingsMu.Unlock()
	s.stagings[sessionID] = ds
}

func (s *Service) abandonStaging(sessionID string) {
	s.stagingsMu.Lock()
	ds, ok := s.stagings[sessionID]
	if ok {
		delete(s.stagings, sessionID)
	}
	s.stagingsMu.Unlock()
	if ok {
		ds.Close()
	}
}

func isContextDone(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
