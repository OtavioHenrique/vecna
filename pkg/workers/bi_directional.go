package workers

import (
	"context"
	"log/slog"
	"maps"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// BiDirectionalWorker is a worker that receives input from a channel and put outputs on a channel
type BiDirectionalWorker struct {
	// worker name to be reported on metrics and logging
	name string
	// intput is the channel that input will be given to this pool of workers
	input <-chan *WorkerData
	// output is the channel which the worker will put output
	output chan<- *WorkerData
	// task to be executed by the worker
	task task.Task
	// number of goroutines to compose this worker pool, each one will listen to the channel and execute tasks
	numWorker int
	logger    *slog.Logger
	metric    metrics.Metric
	closeCh   chan struct{}
}

func NewBiDirectionalWorker(name string, inputCh <-chan *WorkerData, outputCh chan<- *WorkerData, task task.Task, numWorker int, logger *slog.Logger, metric metrics.Metric) *BiDirectionalWorker {
	w := new(BiDirectionalWorker)

	w.name = name
	w.input = inputCh
	w.output = outputCh
	w.task = task
	w.numWorker = numWorker
	w.logger = logger
	w.metric = metric
	w.closeCh = make(chan struct{})

	return w
}

func (w *BiDirectionalWorker) Start(ctx context.Context) {
	w.logger.Info("starting bidirectional worker", "worker_name", w.name)

	for i := 0; i < w.numWorker; i++ {
		go func() {
			for {
				select {
				case msgIn := <-w.input:
					go w.metric.ConsumedMessage(w.name)
					w.logger.Debug("Message Received", "worker_name", w.name)

					go w.metric.TaskRun(w.name)
					resp, err := w.task.Run(ctx, msgIn.Data, msgIn.Metadata, w.name)

					maps.Copy(msgIn.Metadata, resp.Metadata)

					if err != nil {
						w.logger.Error("task error", "worker", w.name, "error", err)
						go w.metric.TaskError(w.name)
					} else {
						w.output <- &WorkerData{Data: resp.Data, Metadata: msgIn.Metadata}
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
}

func (w *BiDirectionalWorker) Stop(ctx context.Context) {
	w.logger.Info("Stopping Worker", "worker_name", w.name)

	close(w.closeCh)
}
