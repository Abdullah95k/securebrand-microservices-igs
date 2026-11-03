// Package api implements igs's REST API (/v1, described in
// api/openapi/igs.v1.yaml): crawl jobs, the profiles
// registered for scheduled crawls, and reads of the stored profiles,
// posts, comments, metrics and media. Keys, scopes and errors come from
// internal/httpapi; data comes from schema ig.
package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	igstaskv1 "github.com/abdullah95k/securebrand-microservices-igs/gen/go/securebrand/igs/task/v1"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/dispatch"
)

// ScopeProfilesWrite registers and changes scheduled profiles.
const ScopeProfilesWrite = "profiles:write"

// Server holds the API's dependencies. DB and ReadDB are pools on schema ig.
type Server struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool // writes
	ReadDB   *pgxpool.Pool // reads (a replica when configured)
	Redis    *redis.Client
	Dispatch *dispatch.Dispatcher
}

// Handler builds the router.
func (s *Server) Handler() http.Handler {
	if s.ReadDB == nil {
		s.ReadDB = s.DB
	}
	auth := httpapi.NewAuthenticator(s.DB, s.Redis, "rl:igapi:", s.Cfg.API.DefaultRatePerMinute)
	req := httpapi.Require
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, httpapi.RequestLog, middleware.Timeout(60*time.Second))
	r.Route("/v1", func(r chi.Router) {
		r.Use(auth.Middleware)
		r.Post("/jobs", req(httpapi.ScopeJobsWrite, s.createJob))
		r.Get("/jobs", req(httpapi.ScopeDataRead, s.listJobs))
		r.Get("/jobs/{id}", req(httpapi.ScopeDataRead, s.getJob))
		r.Post("/jobs/{id}/cancel", req(httpapi.ScopeJobsWrite, s.cancelJob))

		r.Post("/profiles", req(ScopeProfilesWrite, s.registerProfile))
		r.Get("/profiles", req(httpapi.ScopeDataRead, s.listProfiles))
		r.Get("/profiles/{profile}", req(httpapi.ScopeDataRead, s.getProfile))
		r.Patch("/profiles/{profile}", req(ScopeProfilesWrite, s.updateProfile))
		r.Delete("/profiles/{profile}", req(ScopeProfilesWrite, s.deleteProfile))
		r.Get("/profiles/{profile}/posts", req(httpapi.ScopeDataRead, s.listPosts))
		r.Get("/profiles/{profile}/snapshots", req(httpapi.ScopeDataRead, s.profileSnapshots))
		r.Get("/profiles/{profile}/highlights", req(httpapi.ScopeDataRead, s.profileHighlights))

		r.Get("/posts/{post}", req(httpapi.ScopeDataRead, s.getPost))
		r.Get("/posts/{post}/comments", req(httpapi.ScopeDataRead, s.listComments))
		r.Get("/posts/{post}/metrics", req(httpapi.ScopeDataRead, s.postMetrics))
		r.Get("/posts/{post}/media", req(httpapi.ScopeDataRead, s.postMedia))

		r.Post("/erasure-requests", req(httpapi.ScopeAdmin, s.createErasure))
		r.Get("/erasure-requests/{id}", req(httpapi.ScopeAdmin, s.getErasure))

		r.Get("/discovery/candidates", req(httpapi.ScopeAdmin, s.listCandidates))
		r.Post("/discovery/candidates/{username}/reject", req(httpapi.ScopeAdmin, s.rejectCandidate))
		r.Post("/discovery/candidates/{username}/accept", req(httpapi.ScopeAdmin, s.acceptCandidate))

		r.Get("/admin/queries", req(httpapi.ScopeAdmin, s.listQueries))
		r.Put("/admin/queries/{name}", req(httpapi.ScopeAdmin, s.putQuery))
		r.Get("/admin/registry-sync/runs", req(httpapi.ScopeAdmin, s.registrySyncRuns))
	})
	return r
}

// CrawlOptions are the crawl settings a job or a registration may set.
type CrawlOptions = dispatch.CrawlOptions

// Limits applies o over cfg's ig.limits, enforcing the age cap.
func Limits(cfg *config.Config, o CrawlOptions) (*igstaskv1.Limits, error) {
	return dispatch.Limits(cfg, o)
}

// owner is the client whose jobs and registrations the caller sees: itself,
// or everyone (nil) for an admin.
func owner(r *http.Request) *uuid.UUID {
	c := httpapi.ClientFrom(r.Context())
	if c.IsAdmin() {
		return nil
	}
	return &c.ID
}
