// Package requests applies the platform's account requests
// (registry.account_request.v1, securebrand.registry.v1; REQ-019 item 4.3)
// to igs's schedule: add, update the tier, pause, resume, or collect once
// now. Only Instagram's requests are igs's; the others are skipped. The
// consumer runs only with ig.account_requests.enabled.
package requests

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	contentv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/content/v1"
	registryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/registry/v1"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	plog "github.com/abdullah95k/securebrand-microservices-igs/internal/platform/log"
)

// Group is the consumer group of registry.account_request.v1.
const Group = "igs-ig-account-requests"

// Handler applies one request. DB is a pool on schema ig.
type Handler struct {
	DB       *pgxpool.Pool
	Dispatch *dispatch.Dispatcher
	Cfg      *config.Config
}

// errSkip marks a request igs can't apply; it is logged and dropped.
var errSkip = errors.New("skipped")

// Handle implements kafka.Handler: a request igs can't apply is logged and
// dropped, a store failure is retried.
func (h *Handler) Handle(ctx context.Context, rec *kgo.Record) error {
	var r registryv1.AccountRequest
	if err := proto.Unmarshal(rec.Value, &r); err != nil {
		slog.Error("igs account requests: undecodable request", "err", err)
		return nil
	}
	if r.Platform != "ig" {
		return nil
	}
	err := h.Apply(ctx, &r)
	if errors.Is(err, errSkip) {
		slog.Warn("igs account requests: request not applied", "request", r.RequestId, "action", r.Action.String(), "err", err)
		return nil
	}
	return err
}

// Apply carries out one Instagram request.
func (h *Handler) Apply(ctx context.Context, r *registryv1.AccountRequest) error {
	ref, err := Ref(r.Account)
	if err != nil {
		return fmt.Errorf("%w: %w", errSkip, err)
	}
	tier := Tier(r.Tier)
	switch r.Action {
	case registryv1.Action_ACTION_ADD:
		client, err := dispatch.SystemClient(ctx, h.DB, h.client())
		if err != nil {
			return err
		}
		var opts dispatch.CrawlOptions
		if d := int(r.BackfillDays); d > 0 {
			opts.MaxAgeDays = &d
		}
		res, err := h.Dispatch.Register(ctx, h.Cfg, dispatch.Registration{ClientID: client, Ref: ref, Tier: tier, Enabled: true,
			Options: opts, Params: map[string]any{"request_id": r.RequestId, "requested_by": r.RequestedBy, "reason": r.Reason}})
		if err != nil {
			return err
		}
		slog.Info("igs account requests: added", plog.Account(0, ref), "request", r.RequestId, "created", res.Created, "tier", tier)
	case registryv1.Action_ACTION_UPDATE:
		pl, err := dispatch.Place(h.Cfg, tier, 0)
		if err != nil {
			return fmt.Errorf("%w: %w", errSkip, err)
		}
		if err := dispatch.SetPlacement(ctx, h.DB, []string{ref}, pl); err != nil {
			return err
		}
	case registryv1.Action_ACTION_PAUSE, registryv1.Action_ACTION_RESUME:
		n, err := dispatch.SetEnabled(ctx, h.DB, []string{ref}, r.Action == registryv1.Action_ACTION_RESUME)
		if err != nil {
			return err
		}
		if n == 0 {
			return fmt.Errorf("%w: not on the schedule", errSkip)
		}
	case registryv1.Action_ACTION_COLLECT_NOW:
		client, err := dispatch.SystemClient(ctx, h.DB, h.client())
		if err != nil {
			return err
		}
		lim, err := dispatch.Limits(h.Cfg, dispatch.CrawlOptions{})
		if err != nil {
			return err
		}
		// The request ID makes a redelivered request the same job.
		_, _, err = h.Dispatch.Create(ctx, dispatch.Request{ClientID: &client, Kind: dispatch.KindOnDemand,
			Lane: igstaskv1.Lane_LANE_P0, Mode: "auto", ProfileRef: ref, Limits: lim, IdempotencyKey: "account-request:" + r.RequestId,
			Params: map[string]any{"request_id": r.RequestId, "requested_by": r.RequestedBy}, MaxRunningInLane: h.Cfg.API.MaxRunningP0Jobs})
		// Too many jobs running is retried: a slot opens as jobs finish.
		return err
	default:
		return fmt.Errorf("%w: action %v", errSkip, r.Action)
	}
	return nil
}

func (h *Handler) client() string {
	if c := h.Cfg.IG.AccountRequests.Client; c != "" {
		return c
	}
	return "registry"
}

// Ref is igs's profile reference for a requested account: its ID
// (ig:account:<pk>, or the native ID), else its handle, else its URL.
func Ref(a *contentv1.AccountRef) (string, error) {
	if a == nil {
		return "", errors.New("no account")
	}
	if pk, ok := strings.CutPrefix(a.Id, "ig:account:"); ok {
		return task.NormalizeRef("id:" + pk)
	}
	if a.NativeId != "" {
		return task.NormalizeRef("id:" + a.NativeId)
	}
	for _, s := range []string{a.Handle, a.Url} {
		if s = strings.TrimPrefix(strings.TrimSpace(s), "@"); s != "" {
			return task.NormalizeRef(s)
		}
	}
	return "", errors.New("the account has no ID, handle or URL")
}

// Tier is igs's schedule tier for a requested one. Dormant accounts go to
// the long tail, igs's slowest tier; unspecified lets the posting rate
// decide (auto).
func Tier(t registryv1.Tier) string {
	switch t {
	case registryv1.Tier_TIER_HOT:
		return "hot"
	case registryv1.Tier_TIER_WARM:
		return "warm"
	case registryv1.Tier_TIER_LONG_TAIL, registryv1.Tier_TIER_DORMANT:
		return "long_tail"
	}
	return "auto"
}
