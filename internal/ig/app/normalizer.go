package app

import (
	"context"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// NormalizerGroup is igs-normalizer's consumer group (base name).
const NormalizerGroup = "igs-ig-normalizer"

// NewNormalizer builds igs-normalizer: it reads the ig.* entity topics and
// publishes the shared content.* events (normalize), keyed by platform and
// native id. A record that does not decode is counted and skipped: the
// persister quarantines it already.
func NewNormalizer(cfg *config.Config, producer *kafka.Producer) (*kafka.Consumer, error) {
	kcfg, err := clients.Kafka(cfg, "-igs-normalizer")
	if err != nil {
		return nil, err
	}
	prefix := cfg.Kafka.TopicPrefix
	h := func(ctx context.Context, rec *kgo.Record) error {
		base := strings.TrimPrefix(rec.Topic, prefix)
		outs, err := normalize.Record(base, rec.Value)
		if err != nil {
			metrics.Normalized.WithLabelValues(base, "undecodable").Inc()
			slog.Warn("igs normalizer: record skipped", "topic", base, "partition", rec.Partition, "offset", rec.Offset, "err", err)
			return nil
		}
		b := producer.NewBatch()
		for _, o := range outs {
			if err := b.AddProto(ctx, o.Topic, o.Key, o.Msg); err != nil {
				return err
			}
		}
		if err := b.Wait(ctx); err != nil {
			return err
		}
		metrics.Normalized.WithLabelValues(base, "ok").Inc()
		return nil
	}
	return kafka.NewConsumer(kafka.ConsumerConfig{ClientConfig: kcfg, Group: NormalizerGroup, Topics: topics.Entity,
		MaxInflight: 256, MaxInflightPerPartition: 1}, h)
}
