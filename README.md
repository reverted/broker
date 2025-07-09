# Broker Package

[![GoDoc](https://pkg.go.dev/badge/github.com/reverted/broker)](https://pkg.go.dev/github.com/reverted/broker)

A lightweight in-memory event broker for CloudEvents in Go applications.

## Overview

The `broker` package provides a simple pub/sub mechanism for distributing CloudEvents within a Go application. It supports:

- Event publishing and subscribing using the CloudEvents spec
- Type-based event subscription
- Callback-based event handling
- Clean shutdown management

## Installation

```bash
go get github.com/reverted/broker
```

## Usage Examples

### Basic Publishing and Subscribing

```go
package main

import (
	"fmt"
	"github.com/cloudevents/sdk-go/v2"
	"github.com/reverted/broker"
)

func main() {
	// Create a new broker
	b := broker.NewBroker()
	defer b.Shutdown()

	// Subscribe to an event type
	ch := b.Subscribe("user.created")

	// Create and publish an event
	event := cloudevents.NewEvent()
	event.SetID("123")
	event.SetSource("example/uri")
	event.SetType("user.created")
	event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"name": "John Doe", 
		"email": "john@example.com",
	})

	b.Publish(event)

	// Receive the event
	receivedEvent := <-ch
	fmt.Println("Received event:", receivedEvent.Type())
}
```

### Using Callbacks

```go
package main

import (
	"fmt"
	"time"
	"github.com/cloudevents/sdk-go/v2"
	"github.com/reverted/broker"
)

func main() {
	// Create a new broker
	b := broker.NewBroker()
	
	// Subscribe with a callback function
	b.SubscribeFunc("order.created", func(event cloudevents.Event) {
		fmt.Println("New order received:", event.ID())
	})

	// Publish events
	event := cloudevents.NewEvent()
	event.SetID("order-123")
	event.SetSource("orders/api")
	event.SetType("order.created")
	event.SetData(cloudevents.ApplicationJSON, map[string]interface{}{
		"orderId": "123",
		"total": 99.99,
	})

	b.Publish(event)
	
	// Give time for events to be processed
	time.Sleep(100 * time.Millisecond)
	
	// Shutdown when done
	b.Shutdown()
}
```

### Multiple Subscribers

```go
package main

import (
	"fmt"
	"sync"
	"github.com/cloudevents/sdk-go/v2"
	"github.com/reverted/broker"
)

func main() {
	b := broker.NewBroker()
	defer b.Shutdown()

	// Create a wait group to synchronize
	var wg sync.WaitGroup
	wg.Add(2)

	// First subscriber
	b.SubscribeFunc("notification", func(event cloudevents.Event) {
		fmt.Println("Subscriber 1 received:", event.ID())
		wg.Done()
	})

	// Second subscriber
	b.SubscribeFunc("notification", func(event cloudevents.Event) {
		fmt.Println("Subscriber 2 received:", event.ID())
		wg.Done()
	})

	// Publish an event that both will receive
	event := cloudevents.NewEvent()
	event.SetID("notif-123")
	event.SetSource("notification/service")
	event.SetType("notification")
	event.SetData(cloudevents.ApplicationJSON, map[string]string{
		"message": "Hello world",
	})

	b.Publish(event)

	// Wait for both subscribers to process the event
	wg.Wait()
	fmt.Println("All subscribers processed the event")
}
```

### Integration with CloudEvents Receiver

This example shows how to wire the broker into a CloudEvents HTTP receiver, allowing it to process events from external sources:

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	"github.com/reverted/broker"
)

func main() {
	// Create a new broker
	b := broker.NewBroker()

	// Register event handlers with the broker
	b.SubscribeFunc("com.example.event.created", handleCreatedEvent)
	b.SubscribeFunc("com.example.event.updated", handleUpdatedEvent)

	// Set up a CloudEvents HTTP receiver
	p, err := cehttp.New(cehttp.WithPort(8080))
	if err != nil {
		log.Fatalf("Failed to create protocol: %v", err)
	}

	c, err := cloudevents.NewClient(p, client.WithTimeNow(), client.WithUUIDs())
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	ctx, _ := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)

	go func() {
		<-ctx.Done()
		log.Println("Shutdown signal received")
	}()

	log.Println("CloudEvents receiver started on :8080")
	if err := c.StartReceiver(ctx, b.Publish); err != nil {
		log.Printf("Failed to start receiver: %v", err)
	}

	// Shutdown the broker when the server stops
	log.Println("Shutting down broker")
	b.Shutdown()
	log.Println("Shutdown complete")
}

// Event handlers
func handleCreatedEvent(event cloudevents.Event) {
	log.Printf("Processing 'created' event: %s", event.ID())

	// Extract and process the event data
	var data map[string]any
	if err := event.DataAs(&data); err != nil {
		log.Printf("Error parsing event data: %v", err)
		return
	}

	log.Printf("Event data: %v", data)
}

func handleUpdatedEvent(event cloudevents.Event) {
	log.Printf("Processing 'updated' event: %s", event.ID())

	// Extract and process the event data
	var data map[string]any
	if err := event.DataAs(&data); err != nil {
		log.Printf("Error parsing event data: %v", err)
		return
	}

	log.Printf("Event data: %v", data)
}
```

This example demonstrates:
1. Creating a broker and subscribing handlers for specific event types
2. Setting up a CloudEvents HTTP receiver on port 8080
3. Implementing a receiver that forwards events to the broker
4. Handling graceful shutdown with context cancellation and signals
5. Processing different event types with type-specific handlers

To test this example, you can send CloudEvents using `curl`:

```bash
curl -v "http://localhost:8080" \
  -H "Ce-Specversion: 1.0" \
  -H "Ce-Type: com.example.event.created" \
  -H "Ce-Source: curl-test" \
  -H "Ce-Id: 123456" \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello from curl"}'
```

## Features

- **Type-based routing**: Events are delivered only to subscribers of the matching event type
- **Thread-safe**: Can be used safely from multiple goroutines
- **Non-blocking publish**: Publishing events doesn't block, even if subscribers are slow
- **Clean shutdown**: Properly closes all channels and waits for in-flight events

## Limitations

- In-memory only, not suitable for distributed systems
- No persistence or message replay functionality
- No filtering beyond event type matching

## License

[MIT License](LICENSE)
