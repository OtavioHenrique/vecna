package workers

import (
	"context"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// Consumer worker is a worker than simply consumes from a channel and executes tasks passing the input
type ConsumerWorker struct {
	// worker name to be reported on metrics and logging
	name string
	// intput is the channel that input will be given to this pool of workers
	input <-chan *WorkerData
	// task to be executed by the worker
	task task.Task
	// number of goroutines to compose this worker pool, each one will listen to the channel and execute tasks
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
}

func NewConsumerWorker(name string, inputCh <-chan *WorkerData, task task.Task, numWorker int, logger *slog.Logger, metric metrics.Metric) *ConsumerWorker {
	w := new(ConsumerWorker)

	w.name = name
	w.input = inputCh
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *ConsumerWorker) Start(ctx context.Context) {
	w.logger.Info("starting consumer worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.input:
					go w.metric.ConsumedMessage(w.name)
					w.logger.Debug("Message Received", "worker_name", w.name)

					go w.metric.TaskRun(w.name)
					_, err := w.task.Run(ctx, msgIn.Data, msgIn.Metadata, w.name)

					if err != nil {
						go w.metric.TaskError(w.name)
						w.logger.Error("task error", "worker", w.name, "error", err)
					}

					go w.metric.TaskSuccess(w.name)
				case <-w.closeCh:
					return
				}
			}
		}()
	}
}

func (w *ConsumerWorker) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
