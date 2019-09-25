// Copyright (c) 2019 Sick Yoon
// This file is part of gocelery which is released under MIT license.
// See file LICENSE for full license details.

package gocelery

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/argcv/stork/schd"
)

// CeleryWorker represents distributed task worker
type CeleryWorker struct {
	broker          CeleryBroker
	backend         CeleryBackend
	numWorkers      int
	registeredTasks map[string]interface{}
	taskLock        sync.RWMutex
	m               sync.Mutex
	tt              *schd.MultiTaskTicker
}

// NewCeleryWorker returns new celery worker
func NewCeleryWorker(broker CeleryBroker, backend CeleryBackend, numWorkers int) *CeleryWorker {
	mtt := schd.NewMultiTaskTicker()
	mtt.SetPeriod(100 * time.Millisecond)
	mtt.SetNumWorkers(numWorkers)
	return &CeleryWorker{
		broker:          broker,
		backend:         backend,
		registeredTasks: map[string]interface{}{},
		tt:              mtt,
	}
}

func (w *CeleryWorker) SetRateLimitPeriod(rate time.Duration) *CeleryWorker {
	w.tt.SetPeriod(rate)
	return w
}

// StartWorkerWithContext starts celery worker(s) with given parent context
func (w *CeleryWorker) StartWorkerWithContext(ctx context.Context, queues ...string) (err error) {
	w.m.Lock()
	defer w.m.Unlock()
	if w.tt.IsStarted() {
		return errors.New("already started")
	}
	if len(queues) == 0 {
		queues = w.broker.ListQueues()
	}
	w.tt.SetTasks()
	for _, q := range queues {
		w.tt.AddTask(q)
	}
	err = w.tt.Start(ctx, func(c context.Context, param interface{}) {
		queue := param.(string)
		taskMessage, err := w.broker.GetTaskMessageFrom(queue)
		if err != nil || taskMessage == nil {
			return
		}

		// run task
		resultMsg, err := w.RunTask(taskMessage)
		if err != nil {
			log.Printf("failed to run task message %s: %+v", taskMessage.ID, err)
			return
		}
		defer releaseResultMessage(resultMsg)

		// push result to backend
		err = w.backend.SetResult(taskMessage.ID, resultMsg)
		if err != nil {
			log.Printf("failed to push result: %+v", err)
			return
		}
	})
	return
}

// StartWorker starts celery workers
func (w *CeleryWorker) StartWorker(queues ...string) error {
	return w.StartWorkerWithContext(context.Background(), queues...)
}

func (w *CeleryWorker) StopWorkerWithContext(ctx context.Context) (err error) {
	return w.tt.Stop(ctx)
}

// StopWorker stops celery workers
func (w *CeleryWorker) StopWorker() (err error) {
	return w.StopWorkerWithContext(context.TODO())
}

// StopWait waits for celery workers to terminate
func (w *CeleryWorker) StopWait() {
	w.tt.StopWait()
}

// GetNumWorkers returns number of currently running workers
func (w *CeleryWorker) GetNumWorkers() int {
	return w.tt.GetNumWorkers()
}

// Register registers tasks (functions)
func (w *CeleryWorker) Register(name string, task interface{}) {
	w.taskLock.Lock()
	w.registeredTasks[name] = task
	w.taskLock.Unlock()
}

// GetTask retrieves registered task
func (w *CeleryWorker) GetTask(name string) interface{} {
	w.taskLock.RLock()
	defer w.taskLock.RUnlock()
	task, ok := w.registeredTasks[name]
	if !ok {
		return nil
	}
	return task
}

// RunTask runs celery task
func (w *CeleryWorker) RunTask(message *TaskMessage) (*ResultMessage, error) {

	// get task
	task := w.GetTask(message.Task)
	if task == nil {
		return nil, fmt.Errorf("task %s is not registered", message.Task)
	}

	// convert to task interface
	taskInterface, ok := task.(CeleryTask)
	if ok {
		if err := taskInterface.ParseKwargs(message.Kwargs); err != nil {
			return nil, err
		}
		val, err := taskInterface.RunTask()
		if err != nil {
			return nil, err
		}
		return getResultMessage(val), err
	}

	// use reflection to execute function ptr
	taskFunc := reflect.ValueOf(task)
	return runTaskFunc(&taskFunc, message)
}

func runTaskFunc(taskFunc *reflect.Value, message *TaskMessage) (*ResultMessage, error) {

	// check number of arguments
	numArgs := taskFunc.Type().NumIn()
	messageNumArgs := len(message.Args)
	if numArgs != messageNumArgs {
		return nil, fmt.Errorf("Number of task arguments %d does not match number of message arguments %d", numArgs, messageNumArgs)
	}

	// construct arguments
	in := make([]reflect.Value, messageNumArgs)
	for i, arg := range message.Args {
		origType := taskFunc.Type().In(i).Kind()
		msgType := reflect.TypeOf(arg).Kind()
		// special case - convert float64 to int if applicable
		// this is due to json limitation where all numbers are converted to float64
		if origType == reflect.Int && msgType == reflect.Float64 {
			arg = int(arg.(float64))
		}

		in[i] = reflect.ValueOf(arg)
	}

	// call method
	res := taskFunc.Call(in)
	if len(res) == 0 {
		return nil, nil
	}

	return getReflectionResultMessage(&res[0]), nil
}
