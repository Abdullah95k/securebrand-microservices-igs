package kafka_test

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/testenv"
)

// An existing topic is left exactly as it is: neither a dry run nor --apply
// adds partitions or changes its retention, until the option allowing that
// change is set. A missing topic is created only with Apply.
func TestApplyTopicsLeavesExistingTopics(t *testing.T) {
	brokers := testenv.Kafka(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	prefix := testenv.TopicPrefix()

	// Someone else's topic: 2 partitions, kept 30 days.
	existing := kafka.TopicSpec{Name: "shared", Partitions: 2, Retention: 720 * time.Hour}
	if err := kafka.EnsureTopics(ctx, cl, prefix, []kafka.TopicSpec{existing}, 1); err != nil {
		t.Fatal(err)
	}
	// igs's file asks for more partitions and a shorter retention, and for a
	// topic that doesn't exist yet.
	specs := []kafka.TopicSpec{
		{Name: "shared", Partitions: 6, Retention: 24 * time.Hour},
		{Name: "missing", Partitions: 1, Retention: time.Hour},
	}

	// A dry run changes nothing and creates nothing.
	diffs, err := kafka.ApplyTopics(ctx, cl, prefix, specs, 1, kafka.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := diffs[0].Changes; !slices.Equal(got, []string{"add partitions", "lower retention"}) {
		t.Errorf("dry run, existing topic: changes %v", got)
	}
	if got := diffs[1].Changes; !slices.Equal(got, []string{"create"}) || len(diffs[1].Applied) != 0 {
		t.Errorf("dry run, missing topic: changes %v, applied %v", got, diffs[1].Applied)
	}
	assertTopic(ctx, t, brokers, prefix+"shared", 2, 720*time.Hour)
	if exists(ctx, t, brokers, prefix+"missing") {
		t.Fatal("a dry run created a topic")
	}

	// Apply creates the missing topic and holds both changes to the
	// existing one.
	diffs, err = kafka.ApplyTopics(ctx, cl, prefix, specs, 1, kafka.ApplyOptions{Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if got := diffs[0].Held; !slices.Equal(got, []string{"add partitions", "lower retention"}) || len(diffs[0].Applied) != 0 {
		t.Errorf("apply, existing topic: held %v, applied %v", got, diffs[0].Applied)
	}
	assertTopic(ctx, t, brokers, prefix+"shared", 2, 720*time.Hour)
	if !exists(ctx, t, brokers, prefix+"missing") {
		t.Fatal("apply did not create the missing topic")
	}

	// Each change happens only with its own option.
	if _, err := kafka.ApplyTopics(ctx, cl, prefix, specs, 1, kafka.ApplyOptions{Apply: true, GrowPartitions: true}); err != nil {
		t.Fatal(err)
	}
	assertTopic(ctx, t, brokers, prefix+"shared", 6, 720*time.Hour)
	if _, err := kafka.ApplyTopics(ctx, cl, prefix, specs, 1, kafka.ApplyOptions{Apply: true, SetRetention: true}); err != nil {
		t.Fatal(err)
	}
	assertTopic(ctx, t, brokers, prefix+"shared", 6, 24*time.Hour)

	// Now nothing differs.
	diffs, err = kafka.ApplyTopics(ctx, cl, prefix, specs, 1, kafka.ApplyOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range diffs {
		if len(d.Changes) != 0 {
			t.Errorf("%s still differs: %v", d.Topic, d.Changes)
		}
	}
}

// fresh returns an admin client of its own: kgo caches topic metadata for a
// few seconds, so on the client that asked about a topic just before it was
// created (or grown), it still reads as before.
func fresh(t *testing.T, brokers []string) *kadm.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return kadm.NewClient(cl)
}

func exists(ctx context.Context, t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	td, err := fresh(t, brokers).ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	d, ok := td[topic]
	return ok && d.Err == nil
}

func assertTopic(ctx context.Context, t *testing.T, brokers []string, topic string, partitions int, retention time.Duration) {
	t.Helper()
	adm := fresh(t, brokers)
	td, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(td[topic].Partitions); n != partitions {
		t.Errorf("%s: %d partitions, want %d", topic, n, partitions)
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range rcs {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil {
				if *c.Value != strconv.FormatInt(retention.Milliseconds(), 10) {
					t.Errorf("%s: retention.ms %s, want %d", topic, *c.Value, retention.Milliseconds())
				}
				return
			}
		}
	}
	t.Errorf("%s: no retention.ms reported", topic)
}
