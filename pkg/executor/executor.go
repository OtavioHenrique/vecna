package executor

import (
	"context"

	"github.com/otaviohenrique/vecna/pkg/workers"
)

// Executor is the object that will handle all orchestration around Workers created by user
type Executor struct {
	Workers []workers.Worker
}

func NewExecutor(workers []workers.Worker) *Executor {
	ex := new(Executor)

	ex.Workers = workers

	return ex
}

// Start all executor's workers
func (e *Executor) StartWorkers(ctx context.Context) {
	for _, worker := range e.Workers {
		worker.Start(ctx)
	}
}
