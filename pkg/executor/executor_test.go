package executor_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	executor "github.com/otaviohenrique/vecna/pkg/executor"
	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockTask[T any] struct {
	CalledWith []string
	CallCount  int
	mu         sync.Mutex
}

func (t *MockTask[T]) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*task.TaskData[[]byte], error) {

	t.mu.Lock()
	t.CalledWith = append(t.CalledWith, string(input.([]byte)))
	t.CallCount += 1
	msg := []byte(fmt.Sprintf("Called count: %d", t.CallCount))
	t.mu.Unlock()

	return &task.TaskData[[]byte]{Data: msg, Metadata: meta}, nil
}

func TestExecutor_StartWorkers(t *testing.T) {
	type fields struct {
		inputs []any
		logger *slog.Logger
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{"It correct start all workers and creates default queues", fields{
			inputs: []any{
				executor.ExecutorInput[[]byte]{Worker: workers.NewProducerWorker[[]byte](
					"test-producer",
					&MockTask[[]byte]{},
					2,
					slog.New(slog.NewTextHandler(os.Stdout, nil)),
					metrics.NewMockMetrics(),
					500*time.Millisecond,
				), QueueSize: 10},
				executor.ExecutorInput[[]byte]{Worker: workers.NewBiDirectionalWorker[[]byte](
					"test-bidirectional",
					&MockTask[[]byte]{},
					2,
					slog.New(slog.NewTextHandler(os.Stdout, nil)),
					metrics.NewMockMetrics(),
				), QueueSize: 10},
				executor.ExecutorInput[[]byte]{Worker: workers.NewConsumerWorker[[]byte](
					"test-consumer",
					&MockTask[[]byte]{},
					2,
					slog.New(slog.NewTextHandler(os.Stdout, nil)),
					metrics.NewMockMetrics(),
				), QueueSize: 10},
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{ctx: context.TODO()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := executor.NewExecutor(
				tt.fields.logger,
				tt.fields.inputs,
			)
			chs := e.StartWorkers(tt.args.ctx)

			var previousWorker workers.Worker[any]
			for _, input := range tt.fields.inputs {
				if input.Worker.(workers.Worker[any]).Started() != true {
					t.Errorf("Expected worker to be started. Worker %s", input.Worker)
				}

				if previousWorker != nil {
					if input.Worker.(workers.Worker[any]).InputCh() != previousWorker.OutputCh() {
						t.Errorf("Expected input channel to preivous worker output channel. Current worker %s, Previous Worker %s", input.Worker, previousWorker)
					}

					chName := fmt.Sprintf("%s_input", input.Worker.(workers.Worker[any]).Name())
					if chs[chName] != input.Worker.(workers.Worker[any]).OutputCh() {
						t.Errorf("Worker channel is not on returned map. Worker %s, QueueName: %s", input.Worker.(workers.Worker[any]).Name(), chName)
					}
				}

				previousWorker = input.Worker.(workers.Worker[any])
			}
		})
	}
}
