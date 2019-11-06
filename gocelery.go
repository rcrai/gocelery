// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// CeleryClient provides API for sending celery tasks
type CeleryClient struct {
	broker  CeleryBroker
	backend CeleryBackend
	worker  *CeleryWorker
}

// CeleryBroker is interface for celery broker database
type CeleryBroker interface {
	SendCeleryMessage(message *CeleryMessage) error
	SendCeleryMessageTo(queue string, message *CeleryMessage) error
	GetTaskMessage() (message *TaskMessage, error error) // must be non-blocking
	GetTaskMessageFrom(queue string) (message *TaskMessage, error error)
	ListQueues() []string
}

// CeleryBackend is interface for celery backend database
type CeleryBackend interface {
	GetResult(string) (*ResultMessage, error) // must be non-blocking
	SetResult(taskID string, result *ResultMessage) error
	ClearResult(taskID string) error // one additional api
}

// NewCeleryClient creates new celery client
func NewCeleryClient(broker CeleryBroker, backend CeleryBackend, numWorkers int) (*CeleryClient, error) {
	return &CeleryClient{
		broker,
		backend,
		NewCeleryWorker(broker, backend, numWorkers),
	}, nil
}

// Register task
func (cc *CeleryClient) Register(name string, task interface{}) {
	cc.worker.Register(name, task)
}

// StartWorkerWithContext starts celery workers with given parent context
func (cc *CeleryClient) StartWorkerWithContext(ctx context.Context, queues ...string) (err error) {
	return cc.worker.StartWorkerWithContext(ctx, queues...)
}

// StartWorker starts celery workers
func (cc *CeleryClient) StartWorker(queues ...string) error {
	return cc.worker.StartWorker(queues...)
}

// StopWorker stops celery workers
func (cc *CeleryClient) StopWorker() (err error) {
	return cc.worker.StopWorker()
}

// WaitForStopWorker waits for celery workers to terminate
func (cc *CeleryClient) WaitForStopWorker() {
	cc.worker.StopWait()
}

// Delay gets asynchronous result
func (cc *CeleryClient) Delay(task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delay(celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargs(task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delay(celeryTask)
}

// Marshal args as json
func (cc *CeleryClient) DelayJSON(task string, input interface{}) (*AsyncResult, error) {
	// marshal input as JSON
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return cc.Delay(task, string(data))
}

// Delay gets asynchronous result
func (cc *CeleryClient) DelayTo(queue, task string, args ...interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Args = args
	return cc.delayTo(queue, celeryTask)
}

// DelayKwargs gets asynchronous results with argument map
func (cc *CeleryClient) DelayKwargsTo(queue, task string, args map[string]interface{}) (*AsyncResult, error) {
	celeryTask := getTaskMessage(task)
	celeryTask.Kwargs = args
	return cc.delayTo(queue, celeryTask)
}

// Marshal args as json
func (cc *CeleryClient) DelayJSONTo(queue, task string, input interface{}) (*AsyncResult, error) {
	// marshal input as JSON
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	return cc.DelayTo(queue, task, string(data))
}

func (cc *CeleryClient) delay(task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessage(celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

func (cc *CeleryClient) delayTo(queue string, task *TaskMessage) (*AsyncResult, error) {
	defer releaseTaskMessage(task)
	encodedMessage, err := task.Encode()
	if err != nil {
		return nil, err
	}
	celeryMessage := getCeleryMessage(encodedMessage)
	defer releaseCeleryMessage(celeryMessage)
	err = cc.broker.SendCeleryMessageTo(queue, celeryMessage)
	if err != nil {
		return nil, err
	}
	return &AsyncResult{
		TaskID:  task.ID,
		backend: cc.backend,
	}, nil
}

func (cc *CeleryClient) FindResult(taskID string) *AsyncResult {
	return &AsyncResult{
		TaskID:  taskID,
		backend: cc.backend,
	}
}

func (cc *CeleryClient) ClearResult(taskID string) error {
	return AsyncResult{
		TaskID:  taskID,
		backend: cc.backend,
	}.Clear()
}

// given
func (cc *CeleryClient) PollResults(handler func(string, interface{}), taskIDs ...string) {
	for _, taskID := range taskIDs {
		ar := cc.FindResult(taskID)
		val, err := ar.AsyncGet()
		if err != nil {
			continue
		}
		handler(taskID, val)
	}
}

// CeleryTask is an interface that represents actual task
// Passing CeleryTask interface instead of function pointer
// avoids reflection and may have performance gain.
// ResultMessage must be obtained using GetResultMessage()
type CeleryTask interface {
	// ParseKwargs - define a method to parse kwargs
	ParseKwargs(map[string]interface{}) error

	// RunTask - define a method for execution
	RunTask() (interface{}, error)
}

// AsyncResult represents pending result
type AsyncResult struct {
	TaskID  string
	backend CeleryBackend
	result  *ResultMessage
}

// Get gets actual result from backend
// It blocks for period of time set by timeout and returns error if unavailable
func (ar *AsyncResult) Get(timeout time.Duration) (interface{}, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	timeoutChan := time.After(timeout)
	for {
		select {
		case <-timeoutChan:
			err := fmt.Errorf("%v timeout getting result for %s", timeout, ar.TaskID)
			return nil, err
		case <-ticker.C:
			val, err := ar.AsyncGet()
			if err != nil {
				continue
			}
			return val, nil
		}
	}
}

// AsyncGet gets actual result from backend and returns nil if not available
func (ar *AsyncResult) AsyncGet() (interface{}, error) {
	if ar.result != nil {
		return ar.result.Result, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return nil, err
	}
	if val == nil {
		return nil, err
	}
	if val.Status != "SUCCESS" {
		return nil, fmt.Errorf("error response status %v", val)
	}
	ar.result = val
	return val.Result, nil
}

// Ready checks if actual result is ready
func (ar *AsyncResult) Ready() (bool, error) {
	if ar.result != nil {
		return true, nil
	}
	val, err := ar.backend.GetResult(ar.TaskID)
	if err != nil {
		return false, err
	}
	ar.result = val
	return val != nil, nil
}

func (ar *AsyncResult) Clear() (err error) {
	err = ar.backend.ClearResult(ar.TaskID)
	return
}
