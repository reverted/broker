package broker

import (
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func NewNats(logger Logger, url, token, enc string) *broker {

	dh := nats.DisconnectHandler(func(nc *nats.Conn) {
		logger.Warn("Connection Disconnected: ", nc.ConnectedUrl())
	})

	rh := nats.ReconnectHandler(func(nc *nats.Conn) {
		logger.Warn("Connection Reconnected: ", nc.ConnectedUrl())
	})

	ch := nats.ClosedHandler(func(nc *nats.Conn) {
		logger.Warn("Connection Closed: ", nc.LastError())
	})

	var (
		err error
		nc  *nats.Conn
	)

	for _, interval := range []int{0, 1, 2, 5, 10, 30, 60} {
		time.Sleep(time.Duration(interval) * time.Second)

		if nc, err = nats.Connect(url, nats.Token(token), dh, rh, ch); err != nil {
			continue
		}

		break
	}

	if err != nil {
		log.Fatal(err)
	}

	c, err := nats.NewEncodedConn(nc, enc)
	if err != nil {
		log.Fatal(err)
	}

	return New(logger, NewNatsAdapter(c))
}

func NewNatsAdapter(conn *nats.EncodedConn) *adapter {
	return &adapter{conn}
}

type adapter struct {
	*nats.EncodedConn
}

func (self *adapter) Subscribe(subject string, cb interface{}) (interface{}, error) {
	return self.EncodedConn.Subscribe(subject, cb)
}

func (self *adapter) QueueSubscribe(subject string, queue string, cb interface{}) (interface{}, error) {
	return self.EncodedConn.QueueSubscribe(subject, queue, cb)
}
