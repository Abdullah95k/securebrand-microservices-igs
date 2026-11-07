package app

import (
	"context"
	"log/slog"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"

	contracts "github.com/abdullah95k/securebrand-contracts"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/clients"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/config"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/metrics"
)

// NormalizerGroup is igs-normalizer's consumer group (base name). It is
// new with the contract (REQ-019): a new group reads the ig.* topics from
// their start, so the first run re-publishes everything they still hold (7
// days) in the contract's form. Event IDs repeat for the same observation.
const NormalizerGroup = "igs-ig-normalizer-c1"

// Contract headers on every content record (registry/topics.yaml): the
// message's full name, and the contract version.
const (
	HeaderSBType     = "sb-type"
	HeaderSBContract = "sb-contract"
)

// NewNormalizer builds igs-normalizer: it reads the ig.* entity topics and
// publishes the contract's content.* events (normalize), keyed as
// registry/topics.yaml says, with the sb-type and sb-contract headers. vol
// applies the volume rule to Metrics (nil publishes every one). A record
// that does not decode is counted and skipped: the persister quarantines it
// already.
func NewNormalizer(cfg *config.Config, producer *kafka.Producer, vol normalize.Volume) (*kafka.Consumer, error) {
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
		for _, o := range normalize.Filter(ctx, vol, outs) {
			if err := b.AddProto(ctx, o.Topic, o.Key, o.Msg,
				kgo.RecordHeader{Key: HeaderSBType, Value: []byte(proto.MessageName(o.Msg))},
				kgo.RecordHeader{Key: HeaderSBContract, Value: []byte(contracts.Version)}); err != nil {
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
