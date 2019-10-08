// WIP: NOTICE: this broker is NOT tested yet
package gocelery

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

type NSQConfig struct {
	AddrLookupds []string `mapstructure:"addr_lookupds"`
	Addrs        []string `mapstructure:"addrs"`

	DialTimeoutSec int `mapstructure:"dial_timeout_sec"`
	// The server-side message timeout for messages delivered to this client
	MsgTimeoutSec int `mapstructure:"msg_timeout_sec"`

	// Not Configured
	AuthSecret string `mapstructure:"auth_secret"`
}

func (c *NSQConfig) ToNSQConfig() *nsq.Config {
	cfg := nsq.NewConfig()
	cfg.DialTimeout = time.Second * time.Duration(c.DialTimeoutSec)
	cfg.MsgTimeout = time.Second * time.Duration(c.MsgTimeoutSec)
	cfg.MaxInFlight = 200
	if len(c.AuthSecret) > 0 {
		cfg.AuthSecret = c.AuthSecret
	}
	return cfg
}

type NSQHandler struct {
	h func(message *nsq.Message) error
}

func NewNSQHandler(h func(message *nsq.Message) error) *NSQHandler {
	return &NSQHandler{
		h: h,
	}
}

func (h *NSQHandler) HandleMessage(message *nsq.Message) error {
	return h.h(message)
}

type NSQCeleryBroker struct {
	*QueueIterator
	cfg      *NSQConfig
	channel  string
	producer *nsq.Producer
}

//TODO: test
// NewNSQCeleryBroker creates new NSQCeleryBroker based on given config
// NOTE: DON'T USE IT NOW, IT IS WORKING IN PROGRESS!!!
func NewNSQCeleryBroker(cfg *NSQConfig, channel string, queues ...string) *NSQCeleryBroker {

	p, e := nsq.NewProducer(cfg.Addrs[0], cfg.ToNSQConfig())
	if e == nil {
		return nil
	}

	return &NSQCeleryBroker{
		cfg:           cfg,
		channel:       channel,
		producer:      p,
		QueueIterator: NewQueueIterator(queues...),
	}
}

func (nb *NSQCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	return nb.SendCeleryMessageTo(nb.DefaultQueueName(), message)
}

func (nb *NSQCeleryBroker) SendCeleryMessageTo(queue string, message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return nb.producer.Publish(queue, jsonBytes)
}

func (nb *NSQCeleryBroker) GetTaskMessage() (message *TaskMessage, error error) {
	return nb.GetTaskMessageFrom(nb.DefaultQueueName())
}

//TODO: NOT TESTED YET
//TODO: optimize: cache consumer
func (nb *NSQCeleryBroker) GetTaskMessageFrom(queue string) (message *TaskMessage, err error) {
	if cm, err := nsq.NewConsumer(queue, nb.channel, nb.cfg.ToNSQConfig()); err == nil {
		wg := sync.WaitGroup{}
		wg.Add(1)
		message = nil
		h := NewNSQHandler(func(msg *nsq.Message) error {
			// body
			message = &TaskMessage{}
			if err := json.Unmarshal(msg.Body, message); err != nil {
				return err
			} else {
				wg.Done()
				cm.Stop()
			}
			return nil
		})
		cm.AddHandler(h)
		if len(nb.cfg.AddrLookupds) > 0 {
			err = cm.ConnectToNSQLookupds(nb.cfg.AddrLookupds)
		} else if len(nb.cfg.Addrs) > 0 {
			err = cm.ConnectToNSQDs(nb.cfg.Addrs)
		} else {
			cm.Stop()
			return nil, errors.New("address not configured")
		}
		if err != nil {
			return nil, errors.New(fmt.Sprintf("Connect failed: %v", err))
		}
		wg.Wait()
		return message, nil
	} else {
		return nil, errors.New(fmt.Sprintf("NewConsumer failed: %v", err))
	}
}
