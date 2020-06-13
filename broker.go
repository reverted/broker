package broker

import (
	"fmt"
	"log"
	"math/rand"
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

type Logger interface {
	Error(a ...interface{})
	Warn(a ...interface{})
	Info(a ...interface{})
	Debug(a ...interface{})
}

type Connection interface {
	Publish(string, interface{}) error
	PublishRequest(string, string, interface{}) error
	Request(string, interface{}, interface{}, time.Duration) error
	Subscribe(string, interface{}) (interface{}, error)
	QueueSubscribe(string, string, interface{}) (interface{}, error)
	Close()
}

func New(logger Logger, conn Connection) *broker {
	broker := &broker{logger, conn}
	go broker.Heartbeat(30 * time.Second)
	return broker
}

type broker struct {
	Logger
	Connection
}

func (self *broker) Heartbeat(interval time.Duration) {
	topic := fmt.Sprintf("heartbeat:%v", rand.Int())

	self.Subscribe(topic, func(time *time.Time) {
		self.Logger.Info(topic, delimiter, time)
	})

	for tick := range time.Tick(interval) {
		self.Publish(topic, tick)
	}
}

func (self *broker) Publish(subject string, v interface{}) error {
	if err := self.Connection.Publish(subject, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) PublishRequest(subject string, reply string, v interface{}) error {
	if err := self.Connection.PublishRequest(subject, reply, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) Request(subject string, v interface{}, vPtr interface{}, timeout time.Duration) error {
	if err := self.Connection.Request(subject, v, vPtr, timeout); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) Subscribe(subject string, cb interface{}) (interface{}, error) {
	if sub, err := self.Connection.Subscribe(subject, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debug(subject)
		return sub, nil
	}
}

func (self *broker) QueueSubscribe(subject string, queue string, cb interface{}) (interface{}, error) {
	if sub, err := self.Connection.QueueSubscribe(subject, queue, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debug(subject, delimiter, queue)
		return sub, nil
	}
}

const delimiter = " "

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
