package kafka

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TopicSpec describes a topic to create or update.
type TopicSpec struct {
	Name       string        `yaml:"name"`
	Partitions int32         `yaml:"partitions"`
	Retention  time.Duration `yaml:"retention"`
	Replicas   int16         `yaml:"replicas"`
}

// TopicDiff is one topic's difference between its spec and the cluster:
// what applying the spec would change, what was changed, and what was held
// back because the option allowing it was off.
type TopicDiff struct {
	Topic  string
	Exists bool
	// Partitions and retention on the cluster (when it exists) and in the
	// spec. A retention of 0 is unknown (cluster) or unset (spec); -1 is
	// infinite.
	HavePartitions, WantPartitions int32
	HaveRetention, WantRetention   time.Duration
	HaveReplicas, WantReplicas     int16
	// Changes are what the spec asks for: "create", "add partitions",
	// "raise retention", "lower retention", "set retention" (the cluster
	// didn't report one). Applied is the part that was
	// done; Held the part left because its option is off. Notes are
	// differences ApplyTopics never changes (fewer partitions in the spec,
	// another replication factor).
	Changes, Applied, Held, Notes []string
}

// ApplyOptions say how far ApplyTopics may go. The zero value changes
// nothing: it only reports.
type ApplyOptions struct {
	// Apply makes changes; without it ApplyTopics is a dry run.
	Apply bool
	// GrowPartitions allows adding partitions to an existing topic (it
	// moves keys to other partitions, so ordering breaks across the change).
	GrowPartitions bool
	// SetRetention allows changing an existing topic's retention, up or
	// down. Lowering it deletes records sooner.
	SetRetention bool
}

// ApplyTopics compares specs with the cluster and, as far as opt allows,
// creates the missing topics and changes existing ones. Existing topics are
// left exactly as they are unless opt says otherwise. It never deletes a
// topic, removes partitions or changes a replication factor.
func ApplyTopics(ctx context.Context, cl *kgo.Client, prefix string, specs []TopicSpec, defaultReplicas int16, opt ApplyOptions) ([]TopicDiff, error) {
	adm := kadm.NewClient(cl)
	names := make([]string, len(specs))
	for i, s := range specs {
		names[i] = prefix + s.Name
	}
	existing, err := topicStates(ctx, cl, names)
	if err != nil {
		return nil, err
	}
	var present []string
	for _, n := range names {
		if existing[n].exists {
			present = append(present, n)
		}
	}
	retention := map[string]time.Duration{}
	if len(present) > 0 {
		rcs, err := adm.DescribeTopicConfigs(ctx, present...)
		if err != nil {
			return nil, fmt.Errorf("kafka: describe topic configs: %w", err)
		}
		for _, rc := range rcs {
			if rc.Err != nil {
				continue
			}
			for _, c := range rc.Configs {
				if c.Key == "retention.ms" && c.Value != nil {
					if ms, err := strconv.ParseInt(*c.Value, 10, 64); err == nil {
						retention[rc.Name] = msDuration(ms)
					}
				}
			}
		}
	}
	diffs := make([]TopicDiff, 0, len(specs))
	for _, s := range specs {
		name := prefix + s.Name
		repl := s.Replicas
		if repl == 0 {
			repl = defaultReplicas
		}
		d := TopicDiff{Topic: name, WantPartitions: s.Partitions, WantRetention: s.Retention, WantReplicas: repl}
		st := existing[name]
		d.Exists = st.exists
		if !d.Exists {
			d.Changes = append(d.Changes, "create")
			if opt.Apply {
				cfgs := map[string]*string{"cleanup.policy": ptr("delete"), "compression.type": ptr("producer")}
				if s.Retention != 0 {
					cfgs["retention.ms"] = ptr(strconv.FormatInt(durationMs(s.Retention), 10))
				}
				resp, err := adm.CreateTopic(ctx, s.Partitions, repl, cfgs, name)
				switch {
				case err != nil:
					return diffs, fmt.Errorf("kafka: create %s: %w", name, err)
				case errors.Is(resp.Err, kerr.TopicAlreadyExists):
					// Created by someone else since the listing: left as is.
					d.Exists = true
					d.Changes = nil
					d.Notes = append(d.Notes, "created by someone else meanwhile: left as is")
				case resp.Err != nil:
					return diffs, fmt.Errorf("kafka: create %s: %w", name, resp.Err)
				default:
					d.Applied = append(d.Applied, "create")
				}
			}
			diffs = append(diffs, d)
			continue
		}
		d.HavePartitions = st.partitions
		d.HaveReplicas = st.replicas
		d.HaveRetention = retention[name]
		if d.HaveReplicas != repl {
			d.Notes = append(d.Notes, fmt.Sprintf("replication factor %d on the cluster, %d in the spec: left as is", d.HaveReplicas, repl))
		}
		switch {
		case d.HavePartitions < s.Partitions:
			d.Changes = append(d.Changes, "add partitions")
			if opt.Apply && opt.GrowPartitions {
				if _, err := adm.UpdatePartitions(ctx, int(s.Partitions), name); err != nil {
					return diffs, fmt.Errorf("kafka: grow %s: %w", name, err)
				}
				d.Applied = append(d.Applied, "add partitions")
			} else {
				d.Held = append(d.Held, "add partitions")
			}
		case d.HavePartitions > s.Partitions:
			d.Notes = append(d.Notes, "more partitions on the cluster than in the spec: left as is")
		}
		if s.Retention != 0 && d.HaveRetention != s.Retention {
			change := "raise retention"
			switch {
			case d.HaveRetention == 0:
				change = "set retention" // the cluster didn't say
			case s.Retention > 0 && (d.HaveRetention < 0 || d.HaveRetention > s.Retention):
				change = "lower retention"
			}
			d.Changes = append(d.Changes, change)
			if opt.Apply && opt.SetRetention {
				v := strconv.FormatInt(durationMs(s.Retention), 10)
				alter := []kadm.AlterConfig{{Op: kadm.SetConfig, Name: "retention.ms", Value: &v}}
				resps, err := adm.AlterTopicConfigs(ctx, alter, name)
				if err != nil {
					return diffs, fmt.Errorf("kafka: alter %s: %w", name, err)
				}
				for _, r := range resps {
					if r.Err != nil {
						return diffs, fmt.Errorf("kafka: alter %s: %w", name, r.Err)
					}
				}
				d.Applied = append(d.Applied, change)
			} else {
				d.Held = append(d.Held, change)
			}
		}
		diffs = append(diffs, d)
	}
	return diffs, nil
}

// topicState is a topic as the cluster reports it.
type topicState struct {
	exists     bool
	partitions int32
	replicas   int16
}

// topicStates asks the cluster about names directly. kadm answers metadata
// from kgo's cache, which holds a topic's earlier state for a few seconds,
// so a topic created or grown moments ago could read as before.
func topicStates(ctx context.Context, cl *kgo.Client, names []string) (map[string]topicState, error) {
	req := kmsg.NewPtrMetadataRequest()
	for _, n := range names {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(n)
		req.Topics = append(req.Topics, rt)
	}
	req.AllowAutoTopicCreation = false
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("kafka: list topics: %w", err)
	}
	out := make(map[string]topicState, len(names))
	for _, t := range resp.Topics {
		if t.Topic == nil {
			continue
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			if errors.Is(err, kerr.UnknownTopicOrPartition) {
				continue
			}
			return nil, fmt.Errorf("kafka: list topic %s: %w", *t.Topic, err)
		}
		st := topicState{exists: true, partitions: int32(len(t.Partitions))}
		if len(t.Partitions) > 0 {
			st.replicas = int16(len(t.Partitions[0].Replicas))
		}
		out[*t.Topic] = st
	}
	return out, nil
}

// EnsureTopics creates the topics that don't exist yet and leaves existing
// ones exactly as they are (tests and local stacks).
func EnsureTopics(ctx context.Context, cl *kgo.Client, prefix string, specs []TopicSpec, defaultReplicas int16) error {
	_, err := ApplyTopics(ctx, cl, prefix, specs, defaultReplicas, ApplyOptions{Apply: true})
	return err
}

// durationMs and msDuration convert retention, with -1 (infinite) kept.
func durationMs(d time.Duration) int64 {
	if d < 0 {
		return -1
	}
	return d.Milliseconds()
}

func msDuration(ms int64) time.Duration {
	if ms < 0 {
		return -1
	}
	return time.Duration(ms) * time.Millisecond
}

// GroupCaughtUp reports whether group has committed offsets at or beyond the
// given end offsets for every listed partition.
func GroupCaughtUp(ctx context.Context, cl *kgo.Client, group string, ends kadm.ListedOffsets) (bool, error) {
	adm := kadm.NewClient(cl)
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return false, err
	}
	caught := true
	ends.Each(func(lo kadm.ListedOffset) {
		if lo.Err != nil || lo.Offset <= 0 {
			return
		}
		c, ok := committed.Lookup(lo.Topic, lo.Partition)
		if !ok || c.Err != nil || c.At < lo.Offset {
			caught = false
		}
	})
	return caught, nil
}

// EndOffsets returns the current end offsets of topics.
func EndOffsets(ctx context.Context, cl *kgo.Client, topics ...string) (kadm.ListedOffsets, error) {
	return kadm.NewClient(cl).ListEndOffsets(ctx, topics...)
}

func ptr(s string) *string { return &s }
