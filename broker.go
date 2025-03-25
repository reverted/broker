package broker

import (
	"sync"
	"sync/atomic"

	cloudevents "github.com/cloudevents/sdk-go/v2"
)

type Broker struct {
	// event type -> subscribers map
	subscribers map[string][]chan cloudevents.Event

	// all subscribers - these will receive all events
	allSubscribers []chan cloudevents.Event

	// mu protects subscribers
	mu sync.RWMutex
	// wg waits for all events to be processed
	wg sync.WaitGroup

	// shuttingDown is set to true when the bus is being shut down
	shuttingDown atomic.Bool
}

func NewBroker() *Broker {
	return &Broker{
		subscribers:  make(map[string][]chan cloudevents.Event),
		shuttingDown: atomic.Bool{},
	}
}

func (eb *Broker) Subscribe(eventType string) <-chan cloudevents.Event {
	if eb.shuttingDown.Load() {
		// Return a closed channel
		ch := make(chan cloudevents.Event)
		close(ch)
		return ch
	}

	eb.mu.Lock()
	defer eb.mu.Unlock()
	ch := make(chan cloudevents.Event, 10) // Buffered channel

	if eventType == "*" { // Subscribe to all events
		eb.allSubscribers = append(eb.allSubscribers, ch)
	} else {
		eb.subscribers[eventType] = append(eb.subscribers[eventType], ch)
	}

	return ch
}

func (eb *Broker) Publish(event cloudevents.Event) {
	if eb.shuttingDown.Load() {
		return
	}

	eb.mu.RLock()
	defer eb.mu.RUnlock()

	for _, subs := range eb.subscribers[event.Type()] {
		eb.wg.Add(1)
		go func() {
			defer eb.wg.Done()
			subs <- event
		}()
	}

	for _, subs := range eb.allSubscribers {
		eb.wg.Add(1)
		go func() {
			defer eb.wg.Done()
			subs <- event
		}()
	}
}

func (eb *Broker) SubscribeFunc(eventType string, f func(cloudevents.Event)) {
	ch := eb.Subscribe(eventType)
	go func() {
		for event := range ch {
			f(event)
		}
	}()
}

func (eb *Broker) Shutdown() {
	// Mark the bus as shutting down - this prevents new subscriptions
	// and new events from being published
	eb.shuttingDown.Store(true)

	// Wait for all events to be processed
	eb.wg.Wait()

	eb.mu.Lock()
	defer eb.mu.Unlock()

	// Close all typed subscriptions
	for _, subs := range eb.subscribers {
		for _, ch := range subs {
			close(ch)
		}
	}

	// Close all wildcard subscriptions
	for _, ch := range eb.allSubscribers {
		close(ch)
	}
}
