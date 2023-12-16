package metrics

import (
	"reflect"
	"sync"
	"time"
)

// Metric interface defines all metric functions that will be used by Vecna
type Metric interface {
	// EnqueuedMessages will show how many messages are enqueued on a channel
	EnqueuedMessages(msgsNum int, queueName string)
	// ConsumeMessage will be called everytime that a worker consume a message
	ConsumedMessage(workerName string)
	// ProduceMessage will be called everytime that a worker produce a message
	ProducedMessage(workerName string)
	// TaskError will be called everytime that a task returns error
	TaskError(workerName string)
	// TaskSuccess will be called everytime that a task execution succeed
	TaskSuccess(workerName string)
	// TaskRun will be called everytime that a worker calls a task
	TaskRun(workerName string)
	// TaskExecutionTime to measure task execution time in milliseconds
	TaskExecutionTime(workerName string, start time.Time, end time.Time)
}

// ChannelWatcher is a simply Object who will watch all given queues (channels)
// and report them calling EnqueuedMessages from Metric
type ChannelWatcher struct {
	// Channels A map of key strings (channel name) and value a channel
	// Can be used with return value from Executor.StartWorkers()
	Channels map[string]struct{}
	// Ticker is a channel ticker that will trig the watcher to report metrics
	// (Recommended time 10 seconds)
	Ticker <-chan time.Time
	// Metric impl to be used.
	Metric Metric
}

func NewChannelWatcher(channels map[string]struct{}, ticker <-chan time.Time, metric Metric) *ChannelWatcher {
	cw := new(ChannelWatcher)

	cw.Channels = channels
	cw.Metric = metric
	cw.Ticker = ticker

	return cw
}

func (w *ChannelWatcher) Start() {
	go func() {
		for {
			<-w.Ticker

			for key, value := range w.Channels {
				len := reflect.ValueOf(value).Len()
				go w.Metric.EnqueuedMessages(len, key)
			}
		}
	}()
}

// TODO metrics class. Mean to be used if you don't want metrics or don't implemented it yet
type TODO struct{}

func (m *TODO) EnqueuedMessages(msgsNum int, queueName string) {}

func (m *TODO) ConsumedMessage(workerName string) {}

func (m *TODO) ProducedMessage(workerName string) {}

func (m *TODO) TaskError(workerName string) {}

func (m *TODO) TaskSuccess(workerName string) {}

func (m *TODO) TaskRun(workerName string) {}

func (m *TODO) TaskExecutionTime(workerName string, start time.Time, end time.Time) {}

// MockMetric append metrics on maps. Don't use it on production environmnets.
type MockMetric struct {
	EnqueuedMessagesCalled map[string]int
	ConsumedMessageCalled  map[string]int
	ProducedMessageCalled  map[string]int
	TaskErrorCalled        map[string]int
	TaskSuccessCalled      map[string]int
	TaskRunCalled          map[string]int
	taskExecutionTime      map[string]float64
	Lock                   sync.RWMutex
}

func NewMockMetrics() *MockMetric {
	m := new(MockMetric)

	m.EnqueuedMessagesCalled = map[string]int{}
	m.ConsumedMessageCalled = map[string]int{}
	m.ProducedMessageCalled = map[string]int{}
	m.TaskErrorCalled = map[string]int{}
	m.TaskSuccessCalled = map[string]int{}
	m.TaskRunCalled = map[string]int{}
	m.taskExecutionTime = map[string]float64{}
	m.Lock = sync.RWMutex{}

	return m
}

func (m *MockMetric) TaskExecutionTime(workerName string, start time.Time, end time.Time) {

	m.Lock.Lock()
	m.taskExecutionTime[workerName] = float64(start.Sub(end).Milliseconds())
	m.Lock.Unlock()
}

func (m *MockMetric) EnqueuedMessages(msgsNum int, queueName string) {
	m.Lock.Lock()
	m.EnqueuedMessagesCalled[queueName] += 1
	m.Lock.Unlock()
}

func (m *MockMetric) ConsumedMessage(workerName string) {
	m.Lock.Lock()
	m.ConsumedMessageCalled[workerName] += 1
	m.Lock.Unlock()
}

func (m *MockMetric) ProducedMessage(workerName string) {
	m.Lock.Lock()
	m.ProducedMessageCalled[workerName] += 1
	m.Lock.Unlock()
}

func (m *MockMetric) TaskError(workerName string) {
	m.Lock.Lock()
	m.TaskErrorCalled[workerName] += 1
	m.Lock.Unlock()
}

func (m *MockMetric) TaskSuccess(workerName string) {
	m.Lock.Lock()
	m.TaskSuccessCalled[workerName] += 1
	m.Lock.Unlock()
}

func (m *MockMetric) TaskRun(workerName string) {
	m.Lock.Lock()
	m.TaskRunCalled[workerName] += 1
	m.Lock.Unlock()
}
