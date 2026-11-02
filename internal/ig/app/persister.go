package app

import (
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/persist"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// NewPersister builds igs's persister: it reads the ig.* entity topics
// into schema ig (db), quarantining unwritable records through dlq.
func NewPersister(cfg *config.Config, db *pgxpool.Pool, dlq *kafka.Producer, maxBatch int, linger time.Duration) (*persist.Consumer, error) {
	kcfg, err := clients.Kafka(cfg, "-igs-persister")
	if err != nil {
		return nil, err
	}
	return persist.NewConsumer(kcfg, &persist.Writer{DB: db}, dlq, maxBatch, linger)
}
