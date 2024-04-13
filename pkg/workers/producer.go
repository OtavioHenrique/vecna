package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// Producer worker is a worker than simply produces messages on channel based on a empty execution of the given Task
type ProducerWorker[I any, O any] struct {
	// worker name to be reported on metrics and logging
	name string
	// output is a channel which this worker will put tasks results
	Output chan *WorkerData[O]
	// the task to be executed
	task task.Task[I, O]
	// number of workers (goroutines) on this worker pull.
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	// A trigger, which will the frequency which producer will be called
	trigger time.Duration
	closeCh chan struct{}
	started bool
}

func NewProducerWorker[I, O any](name string, task task.Task[I, O], numWorker int, logger *slog.Logger, metric metrics.Metric, trigger time.Duration) *ProducerWorker[I, O] {
	w := new(ProducerWorker[I, O])

	w.name = name
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.trigger = trigger
	w.closeCh = make(chan struct{})

	return w
}

func (w *ProducerWorker[I, O]) Name() string {
	return w.name
}

func (w *ProducerWorker[I, O]) Started() bool {
	return w.started
}

func (w *ProducerWorker[I, O]) InputCh() chan *WorkerData[I] {
	return nil
}

func (w *ProducerWorker[I, O]) OutputCh() chan *WorkerData[O] {
	return w.Output
}

func (w *ProducerWorker[I, O]) AddOutputCh(o chan *WorkerData[O]) {
	w.Output = o
}

func (w *ProducerWorker[I, O]) AddInputCh(i chan *WorkerData[I]) {
	w.logger.Error("Producer worker don't have input channel to add.")
}

func (w *ProducerWorker[I, O]) Start(ctx context.Context) {
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

					var emptyMessage I

					go w.metric.TaskRun(w.name)
					metadata := map[string]interface{}{}
					resp, err := w.task.Run(ctx, emptyMessage, metadata, w.name)

					if err != nil {
						go w.metric.TaskError(w.name)
						w.logger.Error("task error", "worker", w.name, "error", err)
					} else {
						w.Output <- &WorkerData[O]{Data: resp, Metadata: metadata}
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

func (w *ProducerWorker[I, O]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Producer Worker", "worker_name", w.name)

	close(w.closeCh)
}
