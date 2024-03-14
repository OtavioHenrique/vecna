package workers

import (
	"context"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// Consumer worker is a worker than simply consumes from a channel and executes tasks passing the input
type ConsumerWorker[T any, K any] struct {
	// worker name to be reported on metrics and logging
	name string
	// intput is the channel that input will be given to this pool of workers
	Input chan *WorkerData[T]
	// task to be executed by the worker
	task task.Task[any, T]
	// number of goroutines to compose this worker pool, each one will listen to the channel and execute tasks
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

func NewConsumerWorker[T any, K any](name string, task task.Task[K, T], numWorker int, logger *slog.Logger, metric metrics.Metric) *ConsumerWorker[T, K] {
	w := new(ConsumerWorker[T, K])

	w.name = name
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *ConsumerWorker[T, K]) Name() string {
	return w.name
}

func (w *ConsumerWorker[T, K]) Started() bool {
	return w.started
}

func (w *ConsumerWorker[T, K]) InputCh() chan *WorkerData[T] {
	return w.Input
}

func (w *ConsumerWorker[T, K]) OutputCh() chan *WorkerData[K] {
	return nil
}

func (w *ConsumerWorker[T, K]) Start(ctx context.Context) {
	w.logger.Info("starting consumer worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.Input:
					go w.metric.ConsumedMessage(w.name)
					go w.metric.EnqueuedMessages(len(w.Input), w.name+"input")

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

	w.started = true
}

func (w *ConsumerWorker[T, K]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
