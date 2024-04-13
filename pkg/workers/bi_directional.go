package workers

import (
	"context"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// BiDirectionalWorker is a worker that receives input from a channel and put outputs on a channel
type BiDirectionalWorker[I any, O any] struct {
	// worker name to be reported on metrics and logging
	name string
	// intput is the channel that input will be given to this pool of workers
	Input chan *WorkerData[I]
	// output is the channel which the worker will put output
	Output chan *WorkerData[O]
	// task to be executed by the worker
	task task.Task[I, O]
	// number of goroutines to compose this worker pool, each one will listen to the channel and execute tasks
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
	started   bool
}

func NewBiDirectionalWorker[I any, O any](name string, task task.Task[I, O], numWorker int, logger *slog.Logger, metric metrics.Metric) *BiDirectionalWorker[I, O] {
	w := new(BiDirectionalWorker[I, O])

	w.name = name
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *BiDirectionalWorker[I, O]) Name() string {
	return w.name
}

func (w *BiDirectionalWorker[I, O]) Started() bool {
	return w.started
}

func (w *BiDirectionalWorker[I, O]) InputCh() chan *WorkerData[I] {
	return w.Input
}

func (w *BiDirectionalWorker[I, O]) OutputCh() chan *WorkerData[O] {
	return w.Output
}

func (w *BiDirectionalWorker[I, O]) AddOutputCh(o chan *WorkerData[O]) {
	w.Output = o
}

func (w *BiDirectionalWorker[I, O]) AddInputCh(i chan *WorkerData[I]) {
	w.Input = i
}

func (w *BiDirectionalWorker[I, O]) Start(ctx context.Context) {
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
						w.Output <- &WorkerData[O]{Data: resp, Metadata: msgIn.Metadata}
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

func (w *BiDirectionalWorker[I, O]) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
