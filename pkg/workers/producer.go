package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// Producer worker is a worker than simply produces messages on channel based on a empty execution of the given Task
type ProducerWorker[T any, K any] struct {
	// worker name to be reported on metrics and logging
	name string
	// output is a channel which this worker will put tasks results
	Output chan *WorkerData[K]
	// the task to be executed
	task task.Task[T, K]
	// number of workers (goroutines) on this worker pull.
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	// A trigger, which will the frequency which producer will be called
	trigger time.Duration
	closeCh chan struct{}
	started bool
}

func NewProducerWorker[T, K any](name string, task task.Task[T, K], numWorker int, logger *slog.Logger, metric metrics.Metric, trigger time.Duration) *ProducerWorker[T, K] {
	w := new(ProducerWorker[T, K])

	w.name = name
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.trigger = trigger
	w.closeCh = make(chan struct{})

	return w
}

func (w *ProducerWorker[T, K]) Name() string {
	return w.name
}

func (w *ProducerWorker[T, K]) Started() bool {
	return w.started
}

func (w *ProducerWorker[T, K]) InputCh() chan *WorkerData[T] {
	return nil
}

func (w *ProducerWorker[T, K]) OutputCh() chan *WorkerData[K] {
	return w.Output
}

func (w *ProducerWorker[T, K]) AddOutputCh(o chan *WorkerData[K]) {
	w.Output = o
}

func (w *ProducerWorker[T, K]) AddInputCh(i chan *WorkerData[T]) {
	w.logger.Error("Producer worker don't have input channel to add.")
}

func (w *ProducerWorker[T, K]) Start(ctx context.Context) {
	w.logger.Info("starting producer worker", "worker_name", w.name)

	ticker := time.NewTicker(w.trigger)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case <-w.closeCh:
					return
				case <-ticker.C:
					w.logger.Debug("Producing Message", "worker_name", w.name)

					var emptyMessage T

					go w.metric.TaskRun(w.name)
					metadata := map[string]interface{}{}
					resp, err := w.task.Run(ctx, emptyMessage, metadata, w.name)

					if err != nil {
						go w.metric.TaskError(w.name)
						w.logger.Error("task error", "worker", w.name, "error", err)
					} else {
						w.Output <- &WorkerData[K]{Data: resp, Metadata: metadata}
						go func() {
							w.metric.ProducedMessage(w.name)
							w.metric.TaskRun(w.name)
						}()
					}
				}
			}
		}()
	}

	w.started = true
}

func (w *ProducerWorker[T, K]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Producer Worker", "worker_name", w.name)

	close(w.closeCh)
}
