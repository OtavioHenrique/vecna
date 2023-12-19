package executor

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/otaviohenrique/vecna/pkg/workers"
)

type Input interface {
	Start()
}

type ExecutorInput[T any] struct {
	Worker    workers.Worker[T]
	Ch        chan *workers.WorkerData[T]
	QueueSize int
}

// Executor is the object that will handle all orchestration around Workers created by user
type Executor struct {
	inputs []any
	logger *slog.Logger
}

func NewExecutor(logger *slog.Logger, inputs ...any) *Executor {
	ex := new(Executor)

	ex.inputs = inputs
	ex.logger = logger

	return ex
}

// Start all executor's workers and create its queues.
// Return a map of queues names and queues (channel) to be used by watcher if needed
func (e *Executor) StartWorkers(ctx context.Context, i interface{}) {
	//queues := e.connectWorkers()
	e.logger.Info("all workers connected by queues")

	e.startWorkers(ctx, i)

	//return queues
}

func (e *Executor) startWorkers(ctx context.Context, i reflect.Type) {
	for _, input := range e.inputs {
		i := input.(ExecutorInput[i])

		switch i.Worker.(type) {
		case *workers.BiDirectionalWorker[any]:
			w := i.Worker.(*workers.BiDirectionalWorker[any])
			w.Start(ctx)
		case *workers.ProducerWorker[any]:
			w := i.Worker.(*workers.ProducerWorker[any])

			w.Start(ctx)
		case *workers.ConsumerWorker[any]:
			w := i.Worker.(*workers.ConsumerWorker[any])

			w.Start(ctx)
		}

	}
}

func (e *Executor) connectWorkers() map[string]chan *workers.WorkerData[any] {
	var previousWorker workers.Worker[any]

	chCreated := make(map[string]chan *workers.WorkerData[any])

	for _, a := range e.inputs {
		input := a.(ExecutorInput[any])
		var ch chan *workers.WorkerData[any]

		if input.Ch != nil {
			ch = input.Ch
		} else {
			ch = make(chan *workers.WorkerData[any], input.QueueSize)
		}

		switch input.Worker.(type) {
		case *workers.BiDirectionalWorker[any]:
			w := input.Worker.(*workers.BiDirectionalWorker[any])

			w.Input = previousWorker.OutputCh()
			w.Output = ch

			previousWorker = w

			chName := fmt.Sprintf("%s_input", w.Name())
			chCreated[chName] = previousWorker.OutputCh()
		case *workers.ProducerWorker[any]:
			w := input.Worker.(*workers.ProducerWorker[any])

			w.Output = ch
			previousWorker = w
		case *workers.ConsumerWorker[any]:
			w := input.Worker.(*workers.ConsumerWorker[any])

			w.Input = previousWorker.OutputCh()
			previousWorker = w

			chName := fmt.Sprintf("%s_input", w.Name())
			chCreated[chName] = previousWorker.OutputCh()
		}
	}

	return chCreated
}
