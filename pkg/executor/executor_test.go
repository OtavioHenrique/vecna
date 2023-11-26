package executor_test

import (
	"context"
	"testing"

	executor "github.com/otaviohenrique/vecna/pkg/executor"

	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockWorker struct {
	started bool
}

func (mw *MockWorker) Start(_ context.Context) {
	mw.started = true
}

func (mw *MockWorker) Stop(_ context.Context) {}

func TestStartWorkers(t *testing.T) {
	mockWorkers := []workers.Worker{
		&MockWorker{},
		&MockWorker{},
		&MockWorker{},
	}

	executor := executor.NewExecutor(mockWorkers)

	executor.StartWorkers(context.TODO())

	for _, worker := range mockWorkers {
		if mw, ok := worker.(*MockWorker); ok {
			if !mw.started {
				t.Error("Expected Start method to be called on all workers")
			}
		}
	}
}
