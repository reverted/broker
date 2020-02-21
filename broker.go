package broker

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/nats-io/nats.go"
)

type Broker interface {
	Publish(string, interface{}) error
	PublishRequest(string, string, interface{}) error
	Request(string, interface{}, interface{}, time.Duration) error
	Subscribe(string, interface{}) (interface{}, error)
	QueueSubscribe(string, string, interface{}) (interface{}, error)
	Close()
}

type Logger interface {
	Fatal(a ...interface{})
	Fatalf(format string, a ...interface{})
	Error(a ...interface{})
	Errorf(format string, a ...interface{})
	Warn(a ...interface{})
	Warnf(format string, a ...interface{})
	Info(a ...interface{})
	Infof(format string, a ...interface{})
	Debug(a ...interface{})
	Debugf(format string, a ...interface{})
}

func NewNatsFromEnv(logger Logger) *broker {

	url := os.Getenv("ELAPSE_NATS_URL")
	token := os.Getenv("ELAPSE_NATS_TOKEN")

	enc, ok := os.LookupEnv("ELAPSE_NATS_ENC")
	if !ok {
		enc = nats.JSON_ENCODER
	}

	return NewNats(logger, url, token, enc)
}

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

	for _, interval := range []int{0, 1, 2, 5} {
		time.Sleep(time.Duration(interval) * time.Second)

		if nc, err = nats.Connect(url, nats.Token(token), dh, rh, ch); err != nil {
			continue
		}

		break
	}

	if err != nil {
		logger.Fatal(url, delimiter, err)
	}

	c, err := nats.NewEncodedConn(nc, enc)
	if err != nil {
		logger.Fatal(url, delimiter, err)
	}

	broker := &broker{logger, c}
	go broker.Heartbeat(30 * time.Second)
	return broker
}

type broker struct {
	Logger
	Conn *nats.EncodedConn
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
	if err := self.Conn.Publish(subject, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) PublishRequest(subject string, reply string, v interface{}) error {
	if err := self.Conn.PublishRequest(subject, reply, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) Request(subject string, v interface{}, vPtr interface{}, timeout time.Duration) error {
	if err := self.Conn.Request(subject, v, vPtr, timeout); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debug(subject, delimiter, v)
		return nil
	}
}

func (self *broker) Subscribe(subject string, cb interface{}) (interface{}, error) {
	if sub, err := self.Conn.Subscribe(subject, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debug(subject)
		return sub, nil
	}
}

func (self *broker) QueueSubscribe(subject string, queue string, cb interface{}) (interface{}, error) {
	if sub, err := self.Conn.QueueSubscribe(subject, queue, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debug(subject, delimiter, queue)
		return sub, nil
	}
}

func (self *broker) Close() {
	self.Conn.Close()
}

const delimiter = " "
