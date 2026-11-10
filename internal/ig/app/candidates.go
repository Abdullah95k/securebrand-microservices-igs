package app

import (
	"context"

	discoveryv1 "github.com/abdullah95k/securebrand-contracts/gen/go/securebrand/discovery/v1"

	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/discovery"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/normalize"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/ig/topics"
	"github.com/abdullah95k/securebrand-microservices-igs/internal/platform/kafka"
)

// CandidatePublisher publishes discovery's decided candidates on
// discovery.candidate.v1 with the contract's headers (REQ-019 item 4.1).
func CandidatePublisher(p *kafka.Producer) discovery.CandidatePublisher {
	return func(ctx context.Context, key string, c *discoveryv1.Candidate) error {
		b := p.NewBatch()
		if err := b.AddProto(ctx, topics.DiscoveryCandidate, key, c, normalize.Headers(c)...); err != nil {
			return err
		}
		return b.Wait(ctx)
	}
}
