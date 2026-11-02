// Package clients builds the connections every service shares from config:
// Kafka (with kafka.tls and kafka.sasl) and Valkey/Redis (redis.tls). It is
// neutral, so igs's services use it without the copied Facebook wiring.
package clients

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/redisx"
)

// Kafka is a component's Kafka client configuration: id is appended
// to kafka.client_id, and kafka.tls and kafka.sasl secure the connections.
func Kafka(cfg *config.Config, id string) (kafka.ClientConfig, error) {
	tc, err := cfg.Kafka.TLS.Config()
	if err != nil {
		return kafka.ClientConfig{}, fmt.Errorf("kafka: %w", err)
	}
	kc := kafka.ClientConfig{Brokers: cfg.Kafka.Brokers, ClientID: cfg.Kafka.ClientID + id,
		TopicPrefix: cfg.Kafka.TopicPrefix, Linger: cfg.Kafka.ProduceLinger, TLS: tc}
	s := cfg.Kafka.SASL
	switch s.Mechanism {
	case "plain":
		kc.SASL = plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism()
	case "scram-sha-256":
		kc.SASL = scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism()
	case "scram-sha-512":
		kc.SASL = scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()
	}
	return kc, nil
}

// Redis connects to Valkey/Redis (redis.tls for TLS).
func Redis(ctx context.Context, cfg *config.Config) (*redis.Client, error) {
	tc, err := cfg.Redis.TLS.Config()
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return redisx.Open(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.DB, tc)
}
