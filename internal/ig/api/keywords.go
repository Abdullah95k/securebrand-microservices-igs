package api

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/keywords"
)

// The shared Iraq place list discovery matches (ig.discovery_keywords; the
// same routes as fbs's /v1/discovery/keywords). Crawlers and the scheduler
// pick changes up within 5 minutes.

func (s *Server) listKeywords(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := keywords.Filter{Language: q.Get("language"), Group: q.Get("group"), Limit: 10000}
	if v := q.Get("enabled"); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			httpapi.Problem(w, http.StatusBadRequest, "invalid enabled", err.Error())
			return
		}
		f.Enabled = &b
	}
	ks, err := keywords.List(r.Context(), s.ReadDB, f)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, map[string]any{"keywords": ks})
}

type keywordReq struct {
	Term     string `json:"term"`
	Language string `json:"language"`
	Group    string `json:"group"`
	Enabled  *bool  `json:"enabled"`
}

// createKeyword adds a spelling; one that folds to a listed spelling of
// the language returns that keyword with 200 instead of 201.
func (s *Server) createKeyword(w http.ResponseWriter, r *http.Request) {
	var req keywordReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	enabled := req.Enabled == nil || *req.Enabled
	k, created, err := keywords.Add(r.Context(), s.DB, req.Term, req.Language, req.Group, enabled)
	if keywordError(w, err) {
		return
	}
	code := http.StatusOK
	if created {
		code = http.StatusCreated
	}
	httpapi.WriteJSON(w, code, k)
}

type keywordPatch struct {
	Term    *string `json:"term"`
	Group   *string `json:"group"`
	Enabled *bool   `json:"enabled"`
}

func (s *Server) updateKeyword(w http.ResponseWriter, r *http.Request) {
	id, ok := keywordID(w, r)
	if !ok {
		return
	}
	var req keywordPatch
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	k, err := keywords.Set(r.Context(), s.DB, id, keywords.Update{Term: req.Term, Group: req.Group, Enabled: req.Enabled})
	if keywordError(w, err) {
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, k)
}

func (s *Server) deleteKeyword(w http.ResponseWriter, r *http.Request) {
	id, ok := keywordID(w, r)
	if !ok {
		return
	}
	if keywordError(w, keywords.Delete(r.Context(), s.DB, id)) {
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func keywordID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		httpapi.Problem(w, http.StatusBadRequest, "invalid id", "a keyword id is a positive number")
		return 0, false
	}
	return id, true
}

// keywordError writes err's response and reports whether there was one.
func keywordError(w http.ResponseWriter, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, keywords.ErrInvalid):
		httpapi.Problem(w, http.StatusBadRequest, "invalid keyword", err.Error())
	case errors.Is(err, keywords.ErrDuplicate):
		httpapi.Problem(w, http.StatusConflict, "duplicate keyword", err.Error())
	case errors.Is(err, keywords.ErrNotFound):
		httpapi.Problem(w, http.StatusNotFound, "no such keyword", err.Error())
	default:
		httpapi.ServerError(w, err)
	}
	return true
}
