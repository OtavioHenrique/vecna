package executor

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/otaviohenrique/vecna/pkg/workers"
)

type ExecutorInput struct {
	Worker    workers.Worker
	Ch        chan *workers.WorkerData
	QueueSize int
}

// Executor is the object that will handle all orchestration around Workers created by user
type Executor struct {
	inputs []ExecutorInput
	logger *slog.Logger
}

func NewExecutor(inputs []ExecutorInput, logger *slog.Logger) *Executor {
	ex := new(Executor)

	ex.inputs = inputs
	ex.logger = logger

	return ex
}

// Start all executor's workers and create its queues.
// Return a map of queues names and queues (channel) to be used by watcher if needed
func (e *Executor) StartWorkers(ctx context.Context) map[string]chan *workers.WorkerData {
	queues := e.connectWorkers()
	e.logger.Info("all workers connected by queues")

	for _, input := range e.inputs {
		input.Worker.Start(ctx)
	}

	return queues
}

func (e *Executor) connectWorkers() map[string]chan *workers.WorkerData {
	var previousWorker workers.Worker

	chCreated := make(map[string]chan *workers.WorkerData)

	for _, input := range e.inputs {
		var ch chan *workers.WorkerData
		if input.Ch != nil {
			ch = input.Ch
		} else {
			ch = make(chan *workers.WorkerData, input.QueueSize)
		}

		switch input.Worker.(type) {
		case *workers.BiDirectionalWorker:
			w := input.Worker.(*workers.BiDirectionalWorker)

			w.Input = previousWorker.OutputCh()
			w.Output = ch

			previousWorker = w
		case *workers.ProducerWorker:
			w := input.Worker.(*workers.ProducerWorker)

			w.Output = ch
			previousWorker = w
		case *workers.ConsumerWorker:
			w := input.Worker.(*workers.ConsumerWorker)

			w.Input = previousWorker.OutputCh()
			previousWorker = w
		}

		chName := fmt.Sprintf("%s_input", input.Worker.Name())
		chCreated[chName] = ch
	}

	return chCreated
}
