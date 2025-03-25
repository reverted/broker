package broker

import (
	"context"

	cloudevents "github.com/cloudevents/sdk-go/v2/event"
)

type CloudEventSender func(ctx context.Context, event cloudevents.Event)

func CloudEventForwarder(send CloudEventSender) func(e cloudevents.Event) {
	return func(e cloudevents.Event) {
		send(context.Background(), e)
	}
}
