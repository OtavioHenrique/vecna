package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// Producer worker is a worker than simply produces messages on channel based on a empty execution of the given Task
type ProducerWorker struct {
	// worker name to be reported on metrics and logging
	name string
	// output is a channel which this worker will put tasks results
	Output chan *WorkerData
	// the task to be executed
	task task.Task
	// number of workers (goroutines) on this worker pull.
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	// A trigger, which will the frequency which producer will be called
	trigger time.Duration
	closeCh chan struct{}
	started bool
}

func NewProducerWorker(name string, outputCh chan *WorkerData, task task.Task, numWorker int, logger *slog.Logger, metric metrics.Metric, trigger time.Duration) *ProducerWorker {
	w := new(ProducerWorker)

	w.name = name
	w.Output = outputCh
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.trigger = trigger
	w.closeCh = make(chan struct{})

	return w
}

func (w *ProducerWorker) Started() bool {
	return w.started
}

func (w *ProducerWorker) InputCh() chan *WorkerData {
	return nil
}

func (w *ProducerWorker) OutputCh() chan *WorkerData {
	return w.Output
}

func (w *ProducerWorker) Start(ctx context.Context) {
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

					var emptyMessage [0]byte

					go w.metric.TaskRun(w.name)
					resp, err := w.task.Run(ctx, emptyMessage[:], map[string]interface{}{}, w.name)

					if err != nil {
						go w.metric.TaskError(w.name)
						w.logger.Error("task error", "worker", w.name, "error", err)
					} else {
						w.Output <- &WorkerData{Data: resp.Data, Metadata: resp.Metadata}
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

func (w *ProducerWorker) Stop(ctx context.Context) {
	w.logger.Info("Stopping Producer Worker", "worker_name", w.name)

	close(w.closeCh)
}
