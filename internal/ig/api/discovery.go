package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
)

// Candidate is a discovery candidate as GET /v1/discovery/candidates shows
// it.
type Candidate struct {
	Username    string          `json:"username"`
	UserPK      *int64          `json:"user_pk,omitempty"`
	Status      string          `json:"status"`
	Score       *float64        `json:"score,omitempty"`
	Signals     json.RawMessage `json:"signals,omitempty"`
	Tags        int             `json:"tags"`
	Coauthors   int             `json:"coauthors"`
	Mentions    int             `json:"mentions"`
	Comments    int             `json:"comments"`
	IraqContext int             `json:"iraq_context"`
	Priority    float64         `json:"priority"`
	FirstSeenAt time.Time       `json:"first_seen_at"`
	LastSeenAt  time.Time       `json:"last_seen_at"`
	DecidedAt   *time.Time      `json:"decided_at,omitempty"`
	// Read is what the latest vet found: name, biography, links, followers,
	// recent captions.
	Read json.RawMessage `json:"read,omitempty"`
}

const selectCandidate = `SELECT username, user_pk, status, score, signals, tags, coauthors, mentions, comments, iraq_context, priority,
	first_seen_at, last_seen_at, decided_at, vet FROM discovery_candidates`

func scanCandidate(r pgx.CollectableRow) (Candidate, error) {
	var c Candidate
	var sig, read []byte
	err := r.Scan(&c.Username, &c.UserPK, &c.Status, &c.Score, &sig, &c.Tags, &c.Coauthors, &c.Mentions, &c.Comments, &c.IraqContext,
		&c.Priority, &c.FirstSeenAt, &c.LastSeenAt, &c.DecidedAt, &read)
	if len(sig) > 0 {
		c.Signals = sig
	}
	if len(read) > 0 {
		c.Read = read
	}
	return c, err
}

// listCandidates lists discovery candidates, highest score first, then the
// most seen: ?status= (any of new, vetting, registered, below, private,
// gone, failed, rejected), ?min_score=.
func (s *Server) listCandidates(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	var statuses []string
	for _, st := range strings.Split(q.Get("status"), ",") {
		if st = strings.TrimSpace(st); st != "" {
			statuses = append(statuses, st)
		}
	}
	var minScore *float64
	if v := q.Get("min_score"); v != "" {
		var f float64
		if err := json.Unmarshal([]byte(v), &f); err != nil {
			httpapi.Problem(w, http.StatusBadRequest, "invalid min_score", err.Error())
			return
		}
		minScore = &f
	}
	rows, err := s.ReadDB.Query(r.Context(), selectCandidate+` WHERE (cardinality($1::text[]) = 0 OR status = ANY($1))
		AND ($2::float8 IS NULL OR score >= $2) ORDER BY score DESC NULLS LAST, priority DESC LIMIT $3`,
		statuses, minScore, httpapi.LimitParam(r))
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	cs, err := pgx.CollectRows(rows, scanCandidate)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if cs == nil {
		cs = []Candidate{}
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"candidates": cs})
}

func candidateName(r *http.Request) string {
	return strings.ToLower(strings.TrimPrefix(strings.TrimSpace(chi.URLParam(r, "username")), "@"))
}

// rejectCandidate removes a discovered profile from the schedule, unless
// an API client registered it too, and keeps discovery from adding it
// again.
func (s *Server) rejectCandidate(w http.ResponseWriter, r *http.Request) {
	u := candidateName(r)
	if u == "" {
		httpapi.Problem(w, http.StatusBadRequest, "invalid username", "a username is required")
		return
	}
	ctx := r.Context()
	client, err := discovery.Client(ctx, s.DB)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	ref := "u:" + u
	err = pgx.BeginFunc(ctx, s.DB, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO discovery_candidates (username, status, decided_at) VALUES ($1, 'rejected', now())
			ON CONFLICT (username) DO UPDATE SET status = 'rejected', decided_at = now()`, u); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `DELETE FROM profile_registrations WHERE profile_ref = $1 AND api_client_id = $2`, ref, client); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `DELETE FROM profile_schedule s WHERE profile_ref = $1
			AND NOT EXISTS (SELECT 1 FROM profile_registrations g WHERE g.profile_ref = s.profile_ref)`, ref)
		return err
	})
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// acceptCandidate registers a candidate now, whatever its score.
func (s *Server) acceptCandidate(w http.ResponseWriter, r *http.Request) {
	u := candidateName(r)
	if u == "" {
		httpapi.Problem(w, http.StatusBadRequest, "invalid username", "a username is required")
		return
	}
	ctx := r.Context()
	var pk int64
	_ = s.DB.QueryRow(ctx, `SELECT coalesce(user_pk, 0) FROM discovery_candidates WHERE username = $1`, u).Scan(&pk)
	if err := discovery.Register(ctx, s.DB, s.Dispatch, s.Cfg, u, pk); err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if _, err := s.DB.Exec(ctx, `INSERT INTO discovery_candidates (username, status, decided_at) VALUES ($1, 'registered', now())
		ON CONFLICT (username) DO UPDATE SET status = 'registered', decided_at = now()`, u); err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"username": u, "profile_ref": "u:" + u, "registered": true})
}
