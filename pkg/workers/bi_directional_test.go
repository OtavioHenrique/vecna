package workers_test

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"testing"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockTaskBidirectional[T string, K string] struct{}

func (t *MockTaskBidirectional[T, K]) Run(_ context.Context, input T, meta map[string]interface{}, _ string) (K, error) {
	return K(input), nil
}

func TestBiDirectionalWorker_Start(t *testing.T) {
	type fields struct {
		name      string
		Input     chan *workers.WorkerData[string]
		Output    chan *workers.WorkerData[string]
		task      task.Task[string, string]
		numWorker int
		logger    *slog.Logger
		Metrics   metrics.Metric
	}
	tests := []struct {
		name      string
		fields    fields
		inputMsgs []string
	}{
		{"Correct receive messages on input, calls  task and put on output", fields{
			name:      "Test BiDirectionalWorker",
			Input:     make(chan *workers.WorkerData[string]),
			Output:    make(chan *workers.WorkerData[string]),
			numWorker: 3,
			task:      &MockTaskBidirectional[string, string]{},
			logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
			Metrics:   metrics.NewMockMetrics(),
		}, []string{"Input1", "Input2", "Input3", "Input4", "Input5"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := workers.NewBiDirectionalWorker(
				tt.fields.name,
				tt.fields.task,
				tt.fields.numWorker,
				tt.fields.logger,
				tt.fields.Metrics,
			)

			w.Input = tt.fields.Input
			w.Output = tt.fields.Output

			w.Start(context.TODO())

			go func() {
				for i := 0; i < len(tt.inputMsgs); i++ {
					tt.fields.Input <- &workers.WorkerData[string]{Data: tt.inputMsgs[i]}
				}
			}()

			var outputMsgs []string
			for i := 0; i < len(tt.inputMsgs); i++ {
				msg := <-tt.fields.Output

				outputMsgs = append(outputMsgs, string(msg.Data))
			}

			if outputCount, expectedCount := len(outputMsgs), len(tt.inputMsgs); outputCount != expectedCount {
				t.Errorf("BiDirectional worker didn't produced enought messages. Expected %d, Result %d", expectedCount, outputCount)
			}

			for i := 0; i < len(outputMsgs); i++ {
				if !slices.Contains(outputMsgs, tt.inputMsgs[i]) {
					t.Errorf("BiDirectional worker didn't produced the expected message. Expected %s", tt.inputMsgs[i])
				}
			}
		})
	}
}
