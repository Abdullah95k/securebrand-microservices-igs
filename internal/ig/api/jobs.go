package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/task"
)

type createJobReq struct {
	Profile string `json:"profile"`
	// auto (default): incremental when the profile's grid was read before,
	// else full.
	Mode string `json:"mode,omitempty"`
	// RefreshProfile reads the profile itself on an incremental run.
	RefreshProfile bool `json:"refresh_profile,omitempty"`
	CrawlOptions
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobReq
	if err := httpapi.Decode(r, &req); err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid body", err.Error())
		return
	}
	ref, err := task.NormalizeRef(req.Profile)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid profile", err.Error())
		return
	}
	switch req.Mode {
	case "", "auto", "full", "incremental":
	default:
		httpapi.Problem(w, http.StatusBadRequest, "invalid mode", "mode must be auto, full or incremental")
		return
	}
	lim, err := Limits(s.Cfg, req.CrawlOptions)
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid options", err.Error())
		return
	}
	c := httpapi.ClientFrom(r.Context())
	j, created, err := s.Dispatch.Create(r.Context(), dispatch.Request{
		ClientID: &c.ID, Kind: dispatch.KindOnDemand, Lane: igstaskv1.Lane_LANE_P0, Mode: req.Mode, ProfileRef: ref,
		Limits: lim, RefreshProfile: req.RefreshProfile, IdempotencyKey: r.Header.Get("Idempotency-Key"),
		Params: map[string]any{"profile": req.Profile}, MaxRunningInLane: s.Cfg.API.MaxRunningP0Jobs,
		MaxRunningPerClient: s.Cfg.API.MaxRunningP0JobsPerClient,
	})
	switch {
	case errors.Is(err, dispatch.ErrTooManyJobs):
		w.Header().Set("Retry-After", "30")
		httpapi.Problem(w, http.StatusTooManyRequests, "too many running jobs", err.Error())
		return
	case errors.Is(err, dispatch.ErrLaneFull):
		w.Header().Set("Retry-After", "30")
		httpapi.Problem(w, http.StatusTooManyRequests, "on-demand capacity reached", err.Error())
		return
	case errors.Is(err, dispatch.ErrUnknownProfile):
		httpapi.Problem(w, http.StatusUnprocessableEntity, "unknown profile", err.Error())
		return
	case err != nil:
		httpapi.ServerError(w, err)
		return
	}
	code := http.StatusAccepted
	if !created {
		code = http.StatusOK
	}
	w.Header().Set("Location", "/v1/jobs/"+j.ID.String())
	httpapi.WriteJSON(w, code, j)
}

func jobID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(chi.URLParam(r, "id"))
	if err != nil {
		httpapi.Problem(w, http.StatusBadRequest, "invalid job id", err.Error())
		return uuid.Nil, false
	}
	return id, true
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	j, err := s.Dispatch.Get(r.Context(), id, owner(r))
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusOK, j)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	before := time.Now().Add(time.Second)
	if c, ok := httpapi.DecodeCursor(r.URL.Query().Get("cursor")); ok {
		before = c.T
	}
	var ref string
	if p := r.URL.Query().Get("profile"); p != "" {
		var err error
		if ref, err = task.NormalizeRef(p); err != nil {
			httpapi.Problem(w, http.StatusBadRequest, "invalid profile", err.Error())
			return
		}
	}
	limit := httpapi.LimitParam(r)
	js, err := s.Dispatch.List(r.Context(), owner(r), ref, r.URL.Query().Get("status"), before, limit)
	if err != nil {
		httpapi.ServerError(w, err)
		return
	}
	if js == nil {
		js = []*dispatch.Job{}
	}
	out := map[string]any{"jobs": js}
	if len(js) == limit {
		out["next_cursor"] = httpapi.EncodeCursor(js[len(js)-1].CreatedAt, 0)
	}
	httpapi.WriteJSON(w, http.StatusOK, out)
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request) {
	id, ok := jobID(w, r)
	if !ok {
		return
	}
	if err := s.Dispatch.Cancel(r.Context(), id, owner(r)); err != nil {
		httpapi.ServerError(w, err)
		return
	}
	httpapi.WriteJSON(w, http.StatusAccepted, map[string]string{"job_id": id.String(), "status": "cancelling"})
}
