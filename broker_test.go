package broker_test

import (
	"sync/atomic"
	"testing"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/reverted/broker"
)

// Helper function to create a test event
func createTestEvent(eventType string, data string) cloudevents.Event {
	event := cloudevents.NewEvent()
	event.SetID(uuid.New().String())
	event.SetSource("test")
	event.SetType(eventType)
	event.SetData(cloudevents.TextPlain, data) //nolint:errcheck
	return event
}

// Test basic subscription and publishing
func TestBasicEventHandling(t *testing.T) {
	bus := broker.NewBroker()
	defer bus.Shutdown()

	// Subscribe to an event
	ch := bus.Subscribe("test.event")

	// Publish an event
	testEvent := createTestEvent("test.event", "test-data")
	bus.Publish(testEvent)

	// Wait for the event
	select {
	case receivedEvent := <-ch:
		assert.Equal(t, testEvent.ID(), receivedEvent.ID())
		assert.Equal(t, testEvent.Type(), receivedEvent.Type())
		var data string
		err := receivedEvent.DataAs(&data)
		assert.NoError(t, err)
		assert.Equal(t, "test-data", data)
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event")
	}
}

// Test multiple subscribers for the same event type
func TestMultipleSubscribers(t *testing.T) {
	b := broker.NewBroker()
	defer b.Shutdown()

	// Create multiple subscribers
	ch1 := b.Subscribe("test.event")
	ch2 := b.Subscribe("test.event")
	ch3 := b.Subscribe("different.event") // This one shouldn't receive the event

	// Publish an event
	testEvent := createTestEvent("test.event", "test-data")
	b.Publish(testEvent)

	// First subscriber should receive the event
	select {
	case receivedEvent := <-ch1:
		assert.Equal(t, testEvent.ID(), receivedEvent.ID())
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event on first subscriber")
	}

	// Second subscriber should receive the event
	select {
	case receivedEvent := <-ch2:
		assert.Equal(t, testEvent.ID(), receivedEvent.ID())
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event on second subscriber")
	}

	// Third subscriber should not receive the event
	select {
	case <-ch3:
		t.Fatal("Received an event on wrong subscription")
	case <-time.After(100 * time.Millisecond):
		// This is expected
	}
}

// Test subscription with callback function
func TestSubscribeFunc(t *testing.T) {
	b := broker.NewBroker()
	defer b.Shutdown()
	events := make(chan cloudevents.Event, 1)

	// Subscribe with callback
	b.SubscribeFunc("test.event", func(event cloudevents.Event) {
		events <- event
		close(events)
	})

	// Publish an event
	testEvent := createTestEvent("test.event", "test-data")
	b.Publish(testEvent)

	select {
	case event, ok := <-events:
		assert.True(t, ok, "Channel should be open")
		assert.Equal(t, testEvent.ID(), event.ID())
		assert.Equal(t, testEvent.Type(), event.Type())
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for callback execution")
	}
}

// Test publish to multiple event types
func TestMultipleEventTypes(t *testing.T) {
	b := broker.NewBroker()
	defer b.Shutdown()

	// Subscribe to different event types
	ch1 := b.Subscribe("event.type1")
	ch2 := b.Subscribe("event.type2")

	// Publish events of different types
	event1 := createTestEvent("event.type1", "data1")
	event2 := createTestEvent("event.type2", "data2")

	b.Publish(event1)
	b.Publish(event2)

	// Check if events are received by appropriate subscribers
	select {
	case receivedEvent := <-ch1:
		assert.Equal(t, event1.ID(), receivedEvent.ID())
		assert.Equal(t, "event.type1", receivedEvent.Type())
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event.type1")
	}

	select {
	case receivedEvent := <-ch2:
		assert.Equal(t, event2.ID(), receivedEvent.ID())
		assert.Equal(t, "event.type2", receivedEvent.Type())
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event.type2")
	}
}

// Test shutdown behavior
func TestShutdown(t *testing.T) {
	b := broker.NewBroker()

	// Create a channel to signal subscription closure
	closed := make(chan struct{})

	// Create a subscriber
	ch := b.Subscribe("test.event")

	// Start a goroutine to monitor the channel
	go func() {
		// This will unblock when the channel is closed
		for range ch {
			// Just drain the channel
		}
		close(closed)
	}()

	// Publish an event
	b.Publish(createTestEvent("test.event", "test-data"))

	// Shutdown should close all channels and wait for goroutines
	b.Shutdown()

	// After shutdown, we shouldn't be able to subscribe
	closedCh := b.Subscribe("another.event")

	// Check if the returned channel is already closed
	_, ok := <-closedCh
	assert.False(t, ok, "Channel should be closed after shutdown")

	// Wait for our monitoring goroutine to confirm channel closure
	select {
	case <-closed:
		// Channel was properly closed
	case <-time.After(1 * time.Second):
		t.Fatal("Subscription channel wasn't closed after shutdown")
	}
}

// Test that events can't be published after shutdown
func TestNoPublishAfterShutdown(t *testing.T) {
	b := broker.NewBroker()

	// Subscribe to an event
	ch := b.Subscribe("test.event")

	b.Shutdown()

	// Try to publish after shutdown
	event := createTestEvent("test.event", "test-data")
	b.Publish(event)

	// Check that no events are delivered
	select {
	case _, ok := <-ch:
		assert.False(t, ok, "Channel should be closed")
	case <-time.After(100 * time.Millisecond):
		// This is expected - no events should be delivered
	}
}

// Test that multiple subscribers with the same callback function works correctly
func TestMultipleSubscribersSameCallback(t *testing.T) {
	b := broker.NewBroker()
	defer b.Shutdown()

	var counter atomic.Int32

	callback := func(event cloudevents.Event) {
		counter.Add(1)
	}

	// Subscribe the same callback to two different event types
	b.SubscribeFunc("event.type1", callback)
	b.SubscribeFunc("event.type2", callback)

	// Publish one of each event type
	b.Publish(createTestEvent("event.type1", "data"))
	b.Publish(createTestEvent("event.type2", "data"))

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(2), counter.Load(), "Callback should be called exactly twice")
}

func TestSubscribeToAllTypes(t *testing.T) {
	b := broker.NewBroker()
	defer b.Shutdown()

	// Subscribe to all events
	ch := b.Subscribe("*")

	// Publish an event
	event := createTestEvent("test.event", "test-data")
	b.Publish(event)

	// Wait for the event
	select {
	case receivedEvent := <-ch:
		assert.Equal(t, event.ID(), receivedEvent.ID())
		assert.Equal(t, event.Type(), receivedEvent.Type())
		var data string
		err := receivedEvent.DataAs(&data)
		assert.NoError(t, err)
		assert.Equal(t, "test-data", data)
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for event")
	}
}
