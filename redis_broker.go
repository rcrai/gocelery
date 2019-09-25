// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

const (
	DefaultQueueName = "celery"
)

// RedisCeleryBroker is celery broker for redis
type RedisCeleryBroker struct {
	*redis.Pool
	queues []string
	cQueue *int64
}

func (rb *RedisCeleryBroker) next() int64 {
	return atomic.AddInt64(rb.cQueue, 1)
}

func (rb *RedisCeleryBroker) DefaultQueueName() string {
	if len(rb.queues) > 0 {
		return rb.queues[0]
	} else {
		return DefaultQueueName
	}
}

// NewRedisPool creates pool of redis connections from given connection string
func NewRedisPool(uri string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.DialURL(uri)
			if err != nil {
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// NewRedisCeleryBroker creates new RedisCeleryBroker based on given uri
func NewRedisCeleryBroker(uri string, queues ...string) *RedisCeleryBroker {
	if len(queues) == 0 {
		queues = append(queues, DefaultQueueName)
	}
	base := int64(0)
	return &RedisCeleryBroker{
		Pool:   NewRedisPool(uri),
		queues: queues,
		cQueue: &base,
	}
}

// SendCeleryMessage sends CeleryMessage to redis queue
func (cb *RedisCeleryBroker) SendCeleryMessage(message *CeleryMessage) error {
	return cb.SendCeleryMessageTo(cb.DefaultQueueName(), message)
}

func (cb *RedisCeleryBroker) SendCeleryMessageTo(queue string, message *CeleryMessage) error {
	jsonBytes, err := json.Marshal(message)
	if err != nil {
		return err
	}
	conn := cb.Get()
	defer conn.Close()
	_, err = conn.Do("LPUSH", queue, jsonBytes)
	if err != nil {
		return err
	}
	return nil
}

// GetCeleryMessage retrieves celery message from redis queue
func (cb *RedisCeleryBroker) GetCeleryMessage() (msg *CeleryMessage, err error) {
	queues := cb.queues
	lenQueues := len(queues)
	for _ = range queues {
		queue := queues[int(cb.next())%lenQueues]
		msg, err = cb.GetCeleryMessageFrom(queue)
		if err == nil {
			return msg, err
		}
	}
	return
}

func (cb *RedisCeleryBroker) GetCeleryMessageFrom(queue string) (*CeleryMessage, error) {
	conn := cb.Get()
	defer conn.Close()
	messageJSON, err := conn.Do("BLPOP", queue, "1")
	if err != nil {
		return nil, err
	}
	if messageJSON == nil {
		return nil, fmt.Errorf("null message received from redis")
	}
	messageList := messageJSON.([]interface{})
	if string(messageList[0].([]byte)) != queue {
		return nil, fmt.Errorf("not a celery message: %v", messageList[0])
	}
	var message CeleryMessage
	if err := json.Unmarshal(messageList[1].([]byte), &message); err != nil {
		return nil, err
	}
	return &message, nil
}

// GetTaskMessage retrieves task message from redis queue
func (cb *RedisCeleryBroker) GetTaskMessage() (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessage()
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

func (cb *RedisCeleryBroker) GetTaskMessageFrom(queue string) (*TaskMessage, error) {
	celeryMessage, err := cb.GetCeleryMessageFrom(queue)
	if err != nil {
		return nil, err
	}
	return celeryMessage.GetTaskMessage(), nil
}

func (cb *RedisCeleryBroker) ListQueues() []string {
	return append([]string{}, cb.queues...)
}
