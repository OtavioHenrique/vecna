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
type EventBreakerWorker[T []K, K any] struct {
	// Event Name
	name string
	// Input chan
	Input chan *WorkerData[T]
	// Output Chan
	Output chan *WorkerData[K]
	// Number of goroutines executing this task
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

// NewEventBreakerWorker created this worker. Receives: Worker Name, Number of goroutines to execute, logger and metrics
func NewEventBreakerWorker[T []K, K any](name string, numWorker int, logger *slog.Logger, metric metrics.Metric) *EventBreakerWorker[T, K] {
	w := new(EventBreakerWorker[T, K])

	w.name = name
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *EventBreakerWorker[T, K]) Name() string {
	return w.name
}

func (w *EventBreakerWorker[T, K]) Started() bool {
	return w.started
}

func (w *EventBreakerWorker[T, K]) InputCh() chan *WorkerData[T] {
	return w.Input
}

func (w *EventBreakerWorker[T, K]) OutputCh() chan *WorkerData[K] {
	return w.Output
}

func (w *EventBreakerWorker[T, K]) AddOutputCh(o chan *WorkerData[K]) {
	w.Output = o
}

func (w *EventBreakerWorker[T, K]) AddInputCh(i chan *WorkerData[T]) {
	w.Input = i
}

func (w *EventBreakerWorker[T, K]) Start(ctx context.Context) {
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

						w.Output <- &WorkerData[K]{Data: v, Metadata: msgIn.Metadata}
					}
				case <-w.closeCh:
					return
				}
			}
		}()
	}
}

func (w *EventBreakerWorker[T, K]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
