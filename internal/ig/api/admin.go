package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/emit"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/scrape/registry"
)

var queries = registry.In(registry.IGTables)

func (s *Server) listQueries(w http.ResponseWriter, r *http.Request) {
	qs, err := queries.List(r.Context(), s.ReadDB)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"queries": qs})
}

// putQuery sets a registry entry; crawlers pick it up on their next reload.
func (s *Server) putQuery(w http.ResponseWriter, r *http.Request) {
	var q registry.Query
	if err := httpapi.Decode(r, &q); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	q.Name = chi.URLParam(r, "name")
	if q.DocID == "" || q.FriendlyName == "" || q.Parser == "" || len(q.Variables) == 0 {
		httpapi.Problem(w, http.StatusBadRequest, "invalid query", "doc_id, friendly_name, parser and variables are required")
		return
	}
	if q.ParserVersion == 0 {
		q.ParserVersion = 1
	}
	q.Source = "manual"
	if err := queries.Upsert(r.Context(), s.DB, q); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid query", err.Error())
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, q)
}

// Erasure is the API view of an erasure request.
type Erasure struct {
	ID          uuid.UUID       `json:"id"`
	SubjectType string          `json:"subject_type"`
	Status      string          `json:"status"`
	Stats       json.RawMessage `json:"stats,omitempty"`
	Error       *string         `json:"error,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at,omitempty"`
}

type erasureReq struct {
	// SubjectType: author (a commenter: their comments are redacted) or
	// profile (an account: its data is deleted and never read again).
	SubjectType string `json:"subject_type"`
	UserPK      int64  `json:"user_pk,omitempty"`
	Username    string `json:"username,omitempty"`
}

// createErasure queues a GDPR erasure (ADR 0005 §1). Subjects are
// resolved locally: no Instagram request is made.
func (s *Server) createErasure(w http.ResponseWriter, r *http.Request) {
	var req erasureReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ctx := r.Context()
	pk := req.UserPK
	name := strings.TrimPrefix(strings.TrimSpace(req.Username), "@")
	if pk == 0 && name != "" {
		if req.SubjectType == "author" {
			_ = s.DB.QueryRow(ctx, `SELECT author_pk FROM comment_authors WHERE lower(username) = lower($1) AND author_pk IS NOT NULL LIMIT 1`, name).Scan(&pk)
		}
		if pk == 0 {
			if ref, err := task.NormalizeRef(name); err == nil {
				if k, ok := s.Dispatch.Resolve(ctx, ref); ok {
					pk = k.UserPK()
				}
			}
		}
	}
	var key []byte
	switch req.SubjectType {
	case "author":
		if pk != 0 {
			key = emit.AuthorKey([]byte(s.Cfg.IG.Privacy.AuthorHMACKey), pk)
		} else if name != "" {
			_ = s.DB.QueryRow(ctx, `SELECT author_key FROM comment_authors WHERE lower(username) = lower($1) LIMIT 1`, name).Scan(&key)
		}
		if len(key) == 0 {
			httpapi.Problem(w, http.StatusBadRequest, "unknown author", "provide user_pk, or a username we have stored")
			return
		}
	case "profile":
		if pk == 0 {
			httpapi.Problem(w, http.StatusBadRequest, "unknown profile", "provide user_pk, or a username we have stored")
			return
		}
		key = emit.AuthorKey([]byte(s.Cfg.IG.Privacy.AuthorHMACKey), pk)
	default:
		httpapi.Problem(w, http.StatusBadRequest, "invalid subject_type", "subject_type must be author or profile")
		return
	}
	id, _ := uuid.NewV7()
	in, _ := json.Marshal(map[string]any{"user_pk": req.UserPK, "username": req.Username})
	var upk *int64
	if pk != 0 {
		upk = &pk
	}
	e := &Erasure{ID: id, SubjectType: req.SubjectType, Status: "pending", CreatedAt: time.Now().UTC()}
	c := httpapi.ClientFrom(ctx)
	if _, err := s.DB.Exec(ctx, `INSERT INTO erasure_requests (id, api_client_id, subject_type, author_key, user_pk, input, status, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,'pending',$7)`, id, c.ID, req.SubjectType, key, upk, in, e.CreatedAt); err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusAccepted, e)
}

func (s *Server) getErasure(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid id", err.Error())
		return
	}
	var e Erasure
	err = s.DB.QueryRow(r.Context(), `SELECT id, subject_type, status, stats, error, created_at, completed_at
		FROM erasure_requests WHERE id = $1`, id).Scan(&e.ID, &e.SubjectType, &e.Status, &e.Stats, &e.Error, &e.CreatedAt, &e.CompletedAt)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, e)
}
