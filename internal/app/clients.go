package app

import (
	"context"

	"github.com/redis/go-redis/v9"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// KafkaClient is a component's Kafka client configuration (clients.Kafka).
func KafkaClient(cfg *config.Config, id string) (kafka.ClientConfig, error) {
	return clients.Kafka(cfg, id)
}

// OpenRedis connects to Valkey/Redis (clients.Redis).
func OpenRedis(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	return clients.Redis(ctx, cfg)
}
