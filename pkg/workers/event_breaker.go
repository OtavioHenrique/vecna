package workers

import (
	"context"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/metrics"
)

// EventBreakerWorker transforms any given output from previous worker which is an array of data into multiple events to the next worker.
// Example:
// Previous worker output: []string{"a", "b", "c"}
// Output to next worker (After pass throught EventBreakerWorker): "a", "b", "c" (multiple messages)
type EventBreakerWorker[I []O, O any] struct {
	// Event Name
	name string
	// Input chan
	Input chan *WorkerData[I]
	// Output Chan
	Output chan *WorkerData[O]
	// Number of goroutines executing this task
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

// NewEventBreakerWorker created this worker. Receives: Worker Name, Number of goroutines to execute, logger and metrics
func NewEventBreakerWorker[I []O, O any](name string, numWorker int, logger *slog.Logger, metric metrics.Metric) *EventBreakerWorker[I, O] {
	w := new(EventBreakerWorker[I, O])

	w.name = name
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *EventBreakerWorker[I, O]) Name() string {
	return w.name
}

func (w *EventBreakerWorker[I, O]) Started() bool {
	return w.started
}

func (w *EventBreakerWorker[I, O]) InputCh() chan *WorkerData[I] {
	return w.Input
}

func (w *EventBreakerWorker[I, O]) OutputCh() chan *WorkerData[O] {
	return w.Output
}

func (w *EventBreakerWorker[I, O]) AddOutputCh(o chan *WorkerData[O]) {
	w.Output = o
}

func (w *EventBreakerWorker[I, O]) AddInputCh(i chan *WorkerData[I]) {
	w.Input = i
}

func (w *EventBreakerWorker[I, O]) Start(ctx context.Context) {
	w.logger.Info("starting event breaker worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.Input:
					go w.metric.ConsumedMessage(w.name)
					go w.metric.EnqueuedMessages(len(w.Input), w.name+"input")

					w.logger.Debug("Message Received", "worker_name", w.name)

					for _, v := range msgIn.Data {
						go w.metric.ProducedMessage(w.name)

						w.Output <- &WorkerData[O]{Data: v, Metadata: msgIn.Metadata}
					}
				case <-w.closeCh:
					return
				}
			}
		}()
	}
}

func (w *EventBreakerWorker[I, O]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
