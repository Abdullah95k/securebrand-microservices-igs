package api

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/httpapi"
)

// Scopes.
const (
	ScopeJobsWrite  = httpapi.ScopeJobsWrite
	ScopePagesWrite = "pages:write"
	ScopeDataRead   = httpapi.ScopeDataRead
	ScopePIIRead    = httpapi.ScopePIIRead
	ScopeAdmin      = httpapi.ScopeAdmin
)

// Client is an authenticated API client.
type Client = httpapi.Client

// GenerateKey returns a new API key, its prefix and the hash to store.
func GenerateKey() (key, prefix string, hash []byte) { return httpapi.GenerateKey() }

// CreateClient stores a new API client and returns its key.
func CreateClient(ctx context.Context, db *pgxpool.Pool, name string, scopes []string, ratePerMinute int) (string, uuid.UUID, error) {
	return httpapi.CreateClient(ctx, db, name, scopes, ratePerMinute)
}

func clientFrom(ctx context.Context) *Client { return httpapi.ClientFrom(ctx) }

func require(scope string, h http.HandlerFunc) http.HandlerFunc { return httpapi.Require(scope, h) }
