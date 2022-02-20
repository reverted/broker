package broker

import (
	"fmt"
	"math/rand"
	"time"
)

type Logger interface {
	Error(a ...interface{})
	Warn(a ...interface{})
	Infof(a ...interface{})
	Debugf(a ...interface{})
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
		self.Logger.Infof("%v %v", topic, time)
	})

	for tick := range time.Tick(interval) {
		if err := self.Publish(topic, tick); err != nil {
			self.Logger.Error(err)
		}
	}
}

func (self *broker) Publish(subject string, v interface{}) error {
	if err := self.Connection.Publish(subject, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debugf("%v %v", subject, v)
		return nil
	}
}

func (self *broker) PublishRequest(subject string, reply string, v interface{}) error {
	if err := self.Connection.PublishRequest(subject, reply, v); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debugf("%v %v", subject, v)
		return nil
	}
}

func (self *broker) Request(subject string, v interface{}, vPtr interface{}, timeout time.Duration) error {
	if err := self.Connection.Request(subject, v, vPtr, timeout); err != nil {
		self.Logger.Error(err)
		return err
	} else {
		self.Logger.Debugf("%v %v", subject, v)
		return nil
	}
}

func (self *broker) Subscribe(subject string, cb interface{}) (interface{}, error) {
	if sub, err := self.Connection.Subscribe(subject, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debugf("%v", subject)
		return sub, nil
	}
}

func (self *broker) QueueSubscribe(subject string, queue string, cb interface{}) (interface{}, error) {
	if sub, err := self.Connection.QueueSubscribe(subject, queue, cb); err != nil {
		self.Logger.Error(err)
		return nil, err
	} else {
		self.Logger.Debugf("%v %v", subject, queue)
		return sub, nil
	}
}
