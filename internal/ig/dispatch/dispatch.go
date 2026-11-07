// Package dispatch creates igs's crawl jobs: it records the job in schema
// ig and starts its profile task, for the API (on demand) and the
// scheduler (scheduled runs, backfills).
package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/common/v1"
	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/crawl/jobs"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// Job kinds.
const (
	KindOnDemand  = "on_demand"
	KindScheduled = "scheduled"
	KindBackfill  = "backfill"
	// KindDiscovery vets a discovery candidate (M11): a profile read that
	// publishes nothing.
	KindDiscovery = "discovery"
)

var (
	// ErrTooManyJobs: the client already runs its maximum of jobs.
	ErrTooManyJobs = errors.New("too many running jobs for this client")
	// ErrLaneFull: the lane already runs its maximum of jobs.
	ErrLaneFull = errors.New("the on-demand lane is at capacity")
	// ErrUnknownProfile: a profile asked for by id that igs never read, so
	// its username (which the profile and first grid queries take) is
	// unknown.
	ErrUnknownProfile = errors.New("unknown profile id: ask by username first")
)

// Request is a job to create.
type Request struct {
	ClientID   *uuid.UUID
	Kind       string
	Lane       igstaskv1.Lane
	Mode       string // auto | full | incremental
	ProfileRef string // normalised: u:<username> | id:<pk>
	Limits     *igstaskv1.Limits
	// RefreshProfile reads the profile itself on an incremental run (the
	// daily refresh).
	RefreshProfile bool
	IdempotencyKey string
	Params         map[string]any
	// Posts are post tasks started with the job (the scheduled comment
	// refreshes of recent posts).
	Posts []PostRoot
	// MaxRunningInLane caps running jobs in the lane (0: none);
	// MaxRunningPerClient caps the client's (0: none).
	MaxRunningInLane    int
	MaxRunningPerClient int
	// Vet makes the profile task a discovery vet (KindDiscovery).
	Vet bool
}

// PostRoot is a post task started with a job.
type PostRoot struct {
	Scope *igstaskv1.PostScope
	// Mode is the post task's; unset, incremental (a comment refresh). A
	// full one reads the page and every comment (the 30-day re-read).
	Mode commonv1.CrawlMode
}

// Job is a job as the API shows it.
type Job struct {
	ID           uuid.UUID        `json:"job_id"`
	Kind         string           `json:"kind"`
	Lane         string           `json:"lane"`
	Mode         string           `json:"mode"`
	ProfileRef   string           `json:"profile_ref"`
	UserPK       *int64           `json:"user_pk,omitempty"`
	Status       string           `json:"status"`
	Params       json.RawMessage  `json:"params,omitempty"`
	Counters     map[string]int64 `json:"counters,omitempty"`
	Completeness json.RawMessage  `json:"completeness,omitempty"`
	Error        json.RawMessage  `json:"error,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	FinishedAt   *time.Time       `json:"finished_at,omitempty"`
	Pending      *int64           `json:"pending_tasks,omitempty"`
}

// Dispatcher creates and reads jobs. DB is a pool on schema ig.
type Dispatcher struct {
	DB       *pgxpool.Pool
	Jobs     *jobs.Tracker
	Producer *kafka.Producer
}

type execer interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Known is what the store knows of a profile.
type Known struct {
	userPK         int64
	username       string
	highlightsHash []byte
	crawled        bool // a grid pass was recorded
	watermark      *time.Time
}

// UserPK is the profile's user pk (0 when unknown).
func (k Known) UserPK() int64 { return k.userPK }

// Username is the profile's username as last read, or as asked for.
func (k Known) Username() string { return k.username }

// Resolve looks a profile reference up in the store.
func (d *Dispatcher) Resolve(ctx context.Context, ref string) (Known, bool) {
	var k Known
	var err error
	switch {
	case strings.HasPrefix(ref, "id:"):
		k.userPK, _ = strconv.ParseInt(strings.TrimPrefix(ref, "id:"), 10, 64)
		var name *string
		err = d.DB.QueryRow(ctx, `SELECT username, highlights_hash FROM profiles WHERE user_pk = $1`, k.userPK).Scan(&name, &k.highlightsHash)
		if name != nil {
			k.username = *name
		}
	default:
		k.username = strings.TrimPrefix(ref, "u:")
		err = d.DB.QueryRow(ctx, `SELECT user_pk, highlights_hash FROM profiles WHERE lower(username) = lower($1) AND status <> 'suppressed'`,
			k.username).Scan(&k.userPK, &k.highlightsHash)
		if err != nil {
			err = d.DB.QueryRow(ctx, `SELECT user_pk FROM profile_schedule WHERE profile_ref = $1 AND user_pk IS NOT NULL`, ref).Scan(&k.userPK)
		}
	}
	if err != nil || k.userPK == 0 {
		return k, false
	}
	_ = d.DB.QueryRow(ctx, `SELECT last_full_at IS NOT NULL, newest_seen_at FROM crawl_cursors
		WHERE scope_type = 'profile_grid' AND scope_id = $1 AND ordering = 'default'`, k.userPK).Scan(&k.crawled, &k.watermark)
	return k, true
}

// Create records and dispatches a job. With an idempotency key it returns
// the client's existing job instead of a duplicate (created = false).
func (d *Dispatcher) Create(ctx context.Context, r Request) (*Job, bool, error) {
	if r.IdempotencyKey != "" && r.ClientID != nil {
		if j, err := d.byIdempotency(ctx, *r.ClientID, r.IdempotencyKey); err == nil {
			return j, false, nil
		}
	}
	k, found := d.Resolve(ctx, r.ProfileRef)
	if k.username == "" {
		return nil, false, ErrUnknownProfile
	}
	mode := r.Mode
	if mode == "" || mode == "auto" {
		mode = "full"
		if found && k.crawled {
			mode = "incremental"
		}
	}
	cm := commonv1.CrawlMode_CRAWL_MODE_FULL
	if mode == "incremental" {
		cm = commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, false, err
	}
	params := map[string]any{}
	for key, v := range r.Params {
		params[key] = v
	}
	opts := protojson.MarshalOptions{UseProtoNames: true}
	if lb, err := opts.Marshal(r.Limits); err == nil {
		params["limits"] = json.RawMessage(lb)
	}
	if r.RefreshProfile {
		params["refresh_profile"] = true
	}
	pb, _ := json.Marshal(params)
	var upk *int64
	if found {
		upk = &k.userPK
	}
	var idem *string
	if r.IdempotencyKey != "" {
		idem = &r.IdempotencyKey
	}
	j := &Job{ID: id, Kind: r.Kind, Lane: topics.LaneName(r.Lane), Mode: mode, ProfileRef: r.ProfileRef, UserPK: upk,
		Status: "running", Params: pb, CreatedAt: time.Now().UTC()}
	insert := func(db execer) error {
		_, err := db.Exec(ctx, `INSERT INTO jobs (job_id, api_client_id, kind, lane, mode, profile_ref, user_pk, params, status,
			idempotency_key, created_at, dispatched_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'running',$9,$10,$10)`,
			id, r.ClientID, r.Kind, int16(r.Lane), mode, r.ProfileRef, upk, pb, idem, j.CreatedAt)
		return err
	}
	clientCap := r.MaxRunningPerClient > 0 && r.ClientID != nil
	if clientCap || r.MaxRunningInLane > 0 {
		// Count and insert under advisory locks, so concurrent requests on
		// any API replica cannot all pass the caps.
		err = pgx.BeginFunc(ctx, d.DB, func(tx pgx.Tx) error {
			if r.MaxRunningInLane > 0 {
				if err := capped(ctx, tx, "igs:ig:lane:"+topics.LaneName(r.Lane),
					`SELECT count(*) FROM jobs WHERE lane = $1 AND status IN ('running','finalizing')`, int16(r.Lane), r.MaxRunningInLane, ErrLaneFull); err != nil {
					return err
				}
			}
			if clientCap {
				if err := capped(ctx, tx, "igs:ig:client:"+r.ClientID.String(),
					`SELECT count(*) FROM jobs WHERE api_client_id = $1 AND status IN ('running','finalizing')`, *r.ClientID, r.MaxRunningPerClient, ErrTooManyJobs); err != nil {
					return err
				}
			}
			return insert(tx)
		})
		if errors.Is(err, ErrLaneFull) || errors.Is(err, ErrTooManyJobs) {
			return nil, false, err
		}
	} else {
		err = insert(d.DB)
	}
	if err != nil {
		if r.IdempotencyKey != "" && r.ClientID != nil {
			if existing, e2 := d.byIdempotency(ctx, *r.ClientID, r.IdempotencyKey); e2 == nil {
				return existing, false, nil
			}
		}
		return nil, false, fmt.Errorf("dispatch: insert job: %w", err)
	}
	sc := &igstaskv1.ProfileScope{Username: k.username, HighlightsHash: k.highlightsHash, RefreshProfile: r.RefreshProfile, Vet: r.Vet}
	if cm == commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL && k.watermark != nil {
		sc.Watermark = timestamppb.New(*k.watermark)
	}
	root := task.NewProfile(id.String(), r.Lane, cm, r.ProfileRef, k.userPK, r.Limits, sc)
	roots := []*igstaskv1.Task{root}
	for _, p := range r.Posts {
		pm := p.Mode
		if pm == commonv1.CrawlMode_CRAWL_MODE_UNSPECIFIED {
			pm = commonv1.CrawlMode_CRAWL_MODE_INCREMENTAL
		}
		pt := task.NewPost(root, pm, p.Scope)
		roots = append(roots, pt)
		// The refreshed posts' coverage is reported with the job's.
		if _, err := d.Jobs.SetPost(ctx, id.String(), p.Scope.MediaPk, p.Scope.GetTakenAt().GetSeconds(),
			p.Scope.ReportedCommentCount, jobs.PostPending); err != nil {
			d.fail(ctx, id, err)
			return nil, false, err
		}
	}
	ids := make([]string, len(roots))
	recs := make([]*kgo.Record, len(roots))
	for i, t := range roots {
		topic, _ := topics.TaskTopic(t.Kind, t.Lane)
		rec, err := d.Producer.Record(topic, task.ScopeKey(t), t)
		if err != nil {
			d.fail(ctx, id, err)
			return nil, false, err
		}
		ids[i], recs[i] = t.TaskId, rec
	}
	if err := d.Jobs.Spawn(ctx, id.String(), ids...); err != nil {
		d.fail(ctx, id, err)
		return nil, false, err
	}
	if err := d.Producer.ProduceSync(ctx, recs...); err != nil {
		d.fail(ctx, id, err)
		return nil, false, fmt.Errorf("dispatch: produce: %w", err)
	}
	return j, true, nil
}

func capped(ctx context.Context, tx pgx.Tx, lock, count string, arg any, max int, full error) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lock); err != nil {
		return err
	}
	var running int
	if err := tx.QueryRow(ctx, count, arg).Scan(&running); err != nil {
		return err
	}
	if running >= max {
		return full
	}
	return nil
}

func (d *Dispatcher) fail(ctx context.Context, id uuid.UUID, cause error) {
	b, _ := json.Marshal(map[string]string{"message": cause.Error()})
	_, _ = d.DB.Exec(context.WithoutCancel(ctx), `UPDATE jobs SET status = 'failed', error = $2, finished_at = now() WHERE job_id = $1`, id, b)
}

func (d *Dispatcher) byIdempotency(ctx context.Context, client uuid.UUID, key string) (*Job, error) {
	rows, err := d.DB.Query(ctx, selectJob+` WHERE api_client_id = $1 AND idempotency_key = $2`, client, key)
	if err != nil {
		return nil, err
	}
	return pgx.CollectExactlyOneRow(rows, scanJob)
}

const selectJob = `SELECT job_id, kind, lane, mode, profile_ref, user_pk, status, params, counters, completeness, error,
	created_at, finished_at FROM jobs`

func scanJob(row pgx.CollectableRow) (*Job, error) {
	var j Job
	var lane int16
	var counters []byte
	err := row.Scan(&j.ID, &j.Kind, &lane, &j.Mode, &j.ProfileRef, &j.UserPK, &j.Status, &j.Params, &counters,
		&j.Completeness, &j.Error, &j.CreatedAt, &j.FinishedAt)
	if err != nil {
		return nil, err
	}
	j.Lane = topics.LaneName(igstaskv1.Lane(lane))
	if len(counters) > 0 {
		_ = json.Unmarshal(counters, &j.Counters)
	}
	return &j, nil
}

// visibleTo is the condition (on jobs, with the client as $2) for a job a
// client may read: one it created, or a scheduled job (no client) of a
// profile it registered. A NULL client is an admin, who reads every job.
const visibleTo = `($2::uuid IS NULL OR api_client_id = $2 OR (api_client_id IS NULL AND EXISTS (
	SELECT 1 FROM profile_registrations r WHERE r.profile_ref = jobs.profile_ref AND r.api_client_id = $2)))`

// Get returns a job, with live counters while it runs. With owner set, a
// job that client may not read is pgx.ErrNoRows (404); nil reads all.
func (d *Dispatcher) Get(ctx context.Context, id uuid.UUID, owner *uuid.UUID) (*Job, error) {
	rows, err := d.DB.Query(ctx, selectJob+` WHERE job_id = $1 AND `+visibleTo, id, owner)
	if err != nil {
		return nil, err
	}
	j, err := pgx.CollectExactlyOneRow(rows, scanJob)
	if err != nil {
		return nil, err
	}
	if j.Status == "running" || j.Status == "finalizing" || j.Status == "cancelling" {
		if st, err := d.Jobs.Status(ctx, id.String()); err == nil {
			j.Counters = st.Counters
			p := st.Pending
			j.Pending = &p
		}
	}
	return j, nil
}

// List returns recent jobs a client may read, newest first.
func (d *Dispatcher) List(ctx context.Context, owner *uuid.UUID, ref, status string, before time.Time, limit int) ([]*Job, error) {
	q := selectJob + ` WHERE created_at < $1 AND ` + visibleTo
	args := []any{before, owner}
	if ref != "" {
		args = append(args, ref)
		q += fmt.Sprintf(" AND profile_ref = $%d", len(args))
	}
	if status != "" {
		args = append(args, status)
		q += fmt.Sprintf(" AND status = $%d", len(args))
	}
	args = append(args, limit)
	q += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d", len(args))
	rows, err := d.DB.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanJob)
}

// Cancel flags a running job for cancellation. With owner set, only a job
// that client created: a scheduled job serves every client that
// registered its profile, so only an admin cancels it.
func (d *Dispatcher) Cancel(ctx context.Context, id uuid.UUID, owner *uuid.UUID) error {
	tag, err := d.DB.Exec(ctx, `UPDATE jobs SET status = 'cancelling' WHERE job_id = $1 AND status IN ('running','queued')
		AND ($2::uuid IS NULL OR api_client_id = $2)`, id, owner)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return d.Jobs.Cancel(ctx, id.String())
}
