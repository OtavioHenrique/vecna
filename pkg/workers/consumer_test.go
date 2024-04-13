package workers_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockTaskConsumer[T string, K string] struct {
	calledWith []T
	mu         sync.Mutex
}

func (t *MockTaskConsumer[T, K]) CalledWith() []T {
	return t.calledWith
}

func (t *MockTaskConsumer[T, K]) Run(_ context.Context, input T, meta map[string]interface{}, _ string) (K, error) {
	t.mu.Lock()
	t.calledWith = append(t.calledWith, input)
	t.mu.Unlock()

	return "", nil
}

func TestConsumerWorker_Start(t *testing.T) {
	type fields struct {
		name      string
		Input     chan *workers.WorkerData[string]
		Task      *MockTaskConsumer[string, string]
		numWorker int
		logger    *slog.Logger
		Metrics   metrics.Metric
	}
	tests := []struct {
		name      string
		fields    fields
		inputMsgs []string
	}{
		{"Consuming message and running task with it", fields{
			name:      "Test Consumer Worker",
			Input:     make(chan *workers.WorkerData[string]),
			Task:      &MockTaskConsumer[string, string]{},
			numWorker: 3,
			logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
			Metrics:   metrics.NewMockMetrics(),
		}, []string{"Input1", "Input2", "Input3", "Input4", "Input5"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := workers.NewConsumerWorker[string, string](
				tt.fields.name,
				tt.fields.Task,
				tt.fields.numWorker,
				tt.fields.logger,
				tt.fields.Metrics,
			)
			w.Input = tt.fields.Input
			w.Start(context.TODO())

			for i := 0; i < len(tt.inputMsgs); i++ {
				tt.fields.Input <- &workers.WorkerData[string]{Data: tt.inputMsgs[i]}
				time.Sleep(1 * time.Millisecond)
			}

			if consumed, expected := len(tt.fields.Task.CalledWith()), len(tt.inputMsgs); consumed != expected {
				t.Errorf("Consumer should have consumed all input messages. Expected msg count: %d, Result: %d", expected, consumed)
			}
		})
	}
}
