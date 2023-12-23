package workers

import (
	"context"
	"log/slog"
	"reflect"

	"github.com/otaviohenrique/vecna/pkg/metrics"
)

// EventBreakerWorker transforms any given output from previous worker which is an array of data into multiple events to the next worker.
// Example:
// Previous worker output: []string{"a", "b", "c"}
// Output to next worker (After pass throught EventBreakerWorker): "a", "b", "c" (multiple messages)
type EventBreakerWorker struct {
	// Event Name
	name string
	// Input chan
	Input chan *WorkerData
	// Output Chan
	Output chan *WorkerData
	// Number of goroutines executing this task
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

// NewEventBreakerWorker created this worker. Receives: Worker Name, Number of goroutines to execute, logger and metrics
func NewEventBreakerWorker(name string, numWorker int, logger *slog.Logger, metric metrics.Metric) *EventBreakerWorker {
	w := new(EventBreakerWorker)

	w.name = name
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *EventBreakerWorker) Name() string {
	return w.name
}

func (w *EventBreakerWorker) Started() bool {
	return w.started
}

func (w *EventBreakerWorker) InputCh() chan *WorkerData {
	return w.Input
}

func (w *EventBreakerWorker) OutputCh() chan *WorkerData {
	return w.Output
}

func InterfaceToSlice(input interface{}) ([]interface{}, bool) {
	val := reflect.ValueOf(input)

	if val.Kind() != reflect.Slice {
		return nil, false
	}

	result := make([]interface{}, val.Len())

	for i := 0; i < val.Len(); i++ {
		result[i] = val.Index(i).Interface()
	}

	return result, true
}

func (w *EventBreakerWorker) Start(ctx context.Context) {
	w.logger.Info("starting event breaker worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.Input:
					go w.metric.ConsumedMessage(w.name)
					go w.metric.EnqueuedMessages(len(w.Input), w.name+"input")

					w.logger.Debug("Message Received", "worker_name", w.name)

					converted, _ := InterfaceToSlice(msgIn.Data)

					for _, v := range converted {
						go w.metric.ProducedMessage(w.name)
						w.Output <- &WorkerData{Data: v, Metadata: msgIn.Metadata}
					}
				case <-w.closeCh:
					return
				}
			}
		}()
	}
}

func (w *EventBreakerWorker) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
