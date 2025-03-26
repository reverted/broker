package pubsub

import (
	"context"
	"fmt"
	"os"

	cepubsub "github.com/cloudevents/sdk-go/protocol/pubsub/v2"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	cecontext "github.com/cloudevents/sdk-go/v2/context"
	"github.com/reverted/broker"

	"golang.org/x/oauth2/google"
	"google.golang.org/api/compute/v1"
)

type Logger interface {
	Error(...interface{})
	Errorf(string, ...interface{})
	Debugf(string, ...interface{})
	Infof(string, ...interface{})
}

type PubSubForwarder struct {
	topic  string
	client cloudevents.Client
	logger Logger
}

func (p *PubSubForwarder) Send(ctx context.Context, event cloudevents.Event) {
	ctx = cecontext.WithTopic(ctx, p.topic)

	// We configure pubsub to push unwrap payloads - for this to work
	// we need to set the Content-Type of our messages
	//
	// https://cloud.google.com/pubsub/docs/payload-unwrapping

	ctx = cepubsub.WithCustomAttributes(ctx, map[string]string{
		"Content-Type": "application/cloudevents+json; charset=UTF-8",
	})

	if err := p.client.Send(ctx, event); err != nil {
		p.logger.Errorf("failed to publish to pubsub: %v", err)
	}
}

func NewPubSubForwarder(logger Logger, topic string) (broker.EventHandler, error) {
	ctx := context.Background()
	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT")

	if projectID == "" {
		credentials, err := google.FindDefaultCredentials(ctx, compute.ComputeScope)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve project ID: %w", err)
		}
		projectID = credentials.ProjectID
	}

	protocol, err := cepubsub.New(ctx, cepubsub.WithProjectID(projectID))
	if err != nil {
		return nil, fmt.Errorf("failed to instantiate pubsub protocol: %w", err)
	}

	client, err := cloudevents.NewClient(protocol, cloudevents.WithTimeNow(), cloudevents.WithUUIDs())
	if err != nil {
		return nil, fmt.Errorf("failed to create cloudevents client: %w", err)
	}

	pubsub := &PubSubForwarder{
		topic:  topic,
		client: client,
		logger: logger,
	}
	return broker.CloudEventForwarder(pubsub.Send), nil
}
