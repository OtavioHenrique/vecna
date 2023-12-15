package executor_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	executor "github.com/otaviohenrique/vecna/pkg/executor"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

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
				{Worker: &MockProWorker{}, QueueSize: 10},
				{Worker: &MockBiWorker{}, QueueSize: 10},
				{Worker: &MockConWorker{}, QueueSize: 10}},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{ctx: context.TODO()}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := executor.NewExecutor(
				tt.fields.inputs,
				tt.fields.logger,
			)
			e.StartWorkers(tt.args.ctx)

			var previousWorker workers.Worker
			for _, input := range tt.fields.inputs {
				if input.Worker.Started() != true {
					t.Errorf("Expected worker to be started. Worker %s", input.Worker)
				}

				if previousWorker != nil {
					if input.Worker.InputCh() != previousWorker.OutputCh() {
						t.Errorf("Expected input channel to preivous worker output channel. Current worker %s, Previous Worker %s", input.Worker, previousWorker)
					}
				}

				previousWorker = input.Worker
			}
		})
	}
}
