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

type MockTask struct {
	CalledWith []string
	CallCount  int
	mu         sync.Mutex
}

func (t *MockTask) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*task.TaskData, error) {

	t.mu.Lock()
	t.CalledWith = append(t.CalledWith, string(input.([]byte)))
	t.CallCount += 1
	msg := []byte(fmt.Sprintf("Called count: %d", t.CallCount))
	t.mu.Unlock()

	return &task.TaskData{Data: msg, Metadata: meta}, nil
}

func TestExecutor_StartWorkers(t *testing.T) {
	type fields struct {
		inputs []executor.ExecutorInput
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
			inputs: []executor.ExecutorInput{
				{Worker: workers.NewProducerWorker(
					"test-producer",
					&MockTask{},
					2,
					slog.New(slog.NewTextHandler(os.Stdout, nil)),
					metrics.NewMockMetrics(),
					500*time.Millisecond,
				), QueueSize: 10},
				{Worker: workers.NewBiDirectionalWorker(
					"test-bidirectional",
					&MockTask{},
					2,
					slog.New(slog.NewTextHandler(os.Stdout, nil)),
					metrics.NewMockMetrics(),
				), QueueSize: 10},
				{Worker: workers.NewConsumerWorker(
					"test-consumer",
					&MockTask{},
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
				tt.fields.inputs,
				tt.fields.logger,
			)
			chs := e.StartWorkers(tt.args.ctx)

			var previousWorker workers.Worker
			for _, input := range tt.fields.inputs {
				if input.Worker.Started() != true {
					t.Errorf("Expected worker to be started. Worker %s", input.Worker)
				}

				if previousWorker != nil {
					if input.Worker.InputCh() != previousWorker.OutputCh() {
						t.Errorf("Expected input channel to preivous worker output channel. Current worker %s, Previous Worker %s", input.Worker, previousWorker)
					}

					chName := fmt.Sprintf("%s_input", input.Worker.Name())
					if chs[chName] != input.Worker.OutputCh() {
						t.Errorf("Worker channel is not on returned map. Worker %s, QueueName: %s", input.Worker.Name(), chName)
					}
				}

				previousWorker = input.Worker
			}
		})
	}
}
