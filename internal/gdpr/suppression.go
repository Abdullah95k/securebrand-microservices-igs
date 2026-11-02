// Package gdpr implements data-subject protections: pseudonymous author
// keys, the suppression set crawlers consult before publishing (package
// suppress), and the erasure workflow.
package gdpr

import (
	"crypto/hmac"
	"crypto/sha256"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/suppress"
)

// AuthorKey is the stable pseudonymous key of a Facebook author id.
func AuthorKey(secret []byte, authorID string) []byte {
	if authorID == "" {
		return nil
	}
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(authorID))
	return m.Sum(nil)
}

// Suppression is the in-memory set of erased authors and pages.
type Suppression = suppress.Suppression

// NewSuppression reads the suppression table of pool's schema.
func NewSuppression(pool *pgxpool.Pool) *Suppression { return suppress.NewSuppression(pool) }
