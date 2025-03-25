package broker_test

import (
	"context"
	"reflect"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/reverted/broker"
)

type mockSender struct {
	got cloudevents.Event
}

func (m *mockSender) Send(ctx context.Context, event cloudevents.Event) {
	m.got = event
}

func TestCloudEventForwarder(t *testing.T) {
	mock := mockSender{}
	ceForwarder := broker.CloudEventForwarder(mock.Send)

	e := cloudevents.NewEvent()
	e.SetType("test.event")
	e.SetData(cloudevents.TextPlain, "test-data") //nolint:errcheck

	ceForwarder(e)

	got, want := mock.got, e.Clone()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("event forwared wasn't identical")
	}
}
