package workers

import (
	"context"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// BiDirectionalWorker is a worker that receives input from a channel and put outputs on a channel
type BiDirectionalWorker[T any, K any] struct {
	// worker name to be reported on metrics and logging
	name string
	// intput is the channel that input will be given to this pool of workers
	Input chan *WorkerData[T]
	// output is the channel which the worker will put output
	Output chan *WorkerData[K]
	// task to be executed by the worker
	task task.Task[T, K]
	// number of goroutines to compose this worker pool, each one will listen to the channel and execute tasks
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

func NewBiDirectionalWorker[T any, K any](name string, task task.Task[T, K], numWorker int, logger *slog.Logger, metric metrics.Metric) *BiDirectionalWorker[T, K] {
	w := new(BiDirectionalWorker[T, K])

	w.name = name
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *BiDirectionalWorker[T, K]) Name() string {
	return w.name
}

func (w *BiDirectionalWorker[T, K]) Started() bool {
	return w.started
}

func (w *BiDirectionalWorker[T, K]) InputCh() chan *WorkerData[T] {
	return w.Input
}

func (w *BiDirectionalWorker[T, K]) OutputCh() chan *WorkerData[K] {
	return w.Output
}

func (w *BiDirectionalWorker[T, K]) Start(ctx context.Context) {
	w.logger.Info("starting bidirectional worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.Input:
					go w.metric.ConsumedMessage(w.name)
					go w.metric.EnqueuedMessages(len(w.Input), w.name+"input")

					w.logger.Debug("Message Received", "worker_name", w.name)

					go w.metric.TaskRun(w.name)
					resp, err := w.task.Run(ctx, msgIn.Data, msgIn.Metadata, w.name)

					if err != nil {
						w.logger.Error("task error", "worker", w.name, "error", err)
						go w.metric.TaskError(w.name)
					} else {
						w.Output <- &WorkerData[K]{Data: resp, Metadata: msgIn.Metadata}
						go func() {
							w.metric.TaskSuccess(w.name)
							w.metric.ProducedMessage(w.name)
						}()
					}
				case <-w.closeCh:
					return
				}
			}
		}()
	}

	w.started = true
}

func (w *BiDirectionalWorker[T, K]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
