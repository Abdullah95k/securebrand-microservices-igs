// Package api implements the fbs REST API (/v1). The contract is described
// in api/openapi/fbs.v1.yaml.
package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/dispatch"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
)

// Server holds API dependencies.
type Server struct {
	Cfg      *config.Config
	DB       *pgxpool.Pool // writes
	ReadDB   *pgxpool.Pool // reads (replica when configured)
	Redis    *redis.Client
	Dispatch *dispatch.Dispatcher
}

// Handler builds the router.
func (s *Server) Handler() http.Handler {
	if s.ReadDB == nil {
		s.ReadDB = s.DB
	}
	auth := httpapi.NewAuthenticator(s.DB, s.Redis, "rl:api:", s.Cfg.API.DefaultRatePerMinute)
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.Recoverer, requestLog, middleware.Timeout(60*time.Second))
	r.Route("/v1", func(r chi.Router) {
		r.Use(auth.Middleware)
		r.Post("/jobs", require(ScopeJobsWrite, s.createJob))
		r.Get("/jobs", require(ScopeDataRead, s.listJobs))
		r.Get("/jobs/{id}", require(ScopeDataRead, s.getJob))
		r.Post("/jobs/{id}/cancel", require(ScopeJobsWrite, s.cancelJob))
		r.Get("/jobs/{id}/results", require(ScopeDataRead, s.jobResults))

		r.Post("/pages", require(ScopePagesWrite, s.registerPage))
		r.Get("/pages", require(ScopeDataRead, s.listPages))
		r.Get("/pages/{page}", require(ScopeDataRead, s.getPage))
		r.Patch("/pages/{page}", require(ScopePagesWrite, s.updatePage))
		r.Delete("/pages/{page}", require(ScopePagesWrite, s.deletePage))
		r.Get("/pages/{page}/posts", require(ScopeDataRead, s.listPosts))
		r.Get("/pages/{page}/snapshots", require(ScopeDataRead, s.pageSnapshots))

		r.Get("/posts/{post}", require(ScopeDataRead, s.getPost))
		r.Get("/posts/{post}/comments", require(ScopeDataRead, s.listComments))
		r.Get("/posts/{post}/metrics", require(ScopeDataRead, s.postMetrics))
		r.Get("/posts/{post}/media", require(ScopeDataRead, s.postMedia))
		r.Get("/media/{media}", require(ScopeDataRead, s.getMedia))

		r.Post("/erasure-requests", require(ScopeAdmin, s.createErasure))
		r.Get("/erasure-requests/{id}", require(ScopeAdmin, s.getErasure))

		r.Get("/admin/queries", require(ScopeAdmin, s.listQueries))
		r.Put("/admin/queries/{name}", require(ScopeAdmin, s.putQuery))
		r.Get("/admin/registry-sync/runs", require(ScopeAdmin, s.registrySyncRuns))
	})
	return r
}

func requestLog(next http.Handler) http.Handler { return httpapi.RequestLog(next) }

func writeJSON(w http.ResponseWriter, code int, v any) { httpapi.WriteJSON(w, code, v) }

// problem writes an RFC 9457 problem document.
func problem(w http.ResponseWriter, code int, title, detail string) {
	httpapi.Problem(w, code, title, detail)
}

func serverError(w http.ResponseWriter, err error) { httpapi.ServerError(w, err) }

func decode(r *http.Request, v any) error { return httpapi.Decode(r, v) }

func limitParam(r *http.Request) int { return httpapi.LimitParam(r) }

type cursorPos = httpapi.CursorPos

func encodeCursor(t time.Time, id int64) string { return httpapi.EncodeCursor(t, id) }

func decodeCursor(s string) (cursorPos, bool) { return httpapi.DecodeCursor(s) }

func pathInt(r *http.Request, name string) (int64, bool) { return httpapi.PathInt(r, name) }

func boolParam(r *http.Request, name string) bool { return httpapi.BoolParam(r, name) }
