package workers_test

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockTaskConsumer[T []byte] struct {
	calledWith []string
	mu         sync.Mutex
}

func (t *MockTaskConsumer[T]) CalledWith() []string {
	return t.calledWith
}

func (t *MockTaskConsumer[T]) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*task.TaskData[[]byte], error) {
	t.mu.Lock()
	t.calledWith = append(t.calledWith, string(input.([]byte)))
	t.mu.Unlock()

	return &task.TaskData[[]byte]{Data: []byte(""), Metadata: meta}, nil
}

func TestConsumerWorker_Start(t *testing.T) {
	type fields struct {
		name      string
		Input     chan *workers.WorkerData[[]byte]
		Task      task.Task[[]byte]
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
			Input:     make(chan *workers.WorkerData[[]byte]),
			Task:      &MockTaskConsumer[[]byte]{},
			numWorker: 3,
			logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
			Metrics:   metrics.NewMockMetrics(),
		}, []string{"Input1", "Input2", "Input3", "Input4", "Input5"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := workers.NewConsumerWorker(
				tt.fields.name,
				tt.fields.Task,
				tt.fields.numWorker,
				tt.fields.logger,
				tt.fields.Metrics,
			)
			w.Input = tt.fields.Input
			w.Start(context.TODO())

			doneCh := make(chan struct{})

			go func() {
				for i := 0; i < len(tt.inputMsgs); i++ {
					tt.fields.Input <- &workers.WorkerData[[]byte]{Data: []byte(tt.inputMsgs[i])}
					time.Sleep(1 * time.Millisecond)
				}

				doneCh <- struct{}{}
			}()

			<-doneCh

			task := tt.fields.Task.(*MockTaskConsumer[[]byte])
			if consumed, expected := len(task.CalledWith()), len(tt.inputMsgs); consumed != expected {
				t.Errorf("Consumer should have consumed all input messages. Expected msg count: %d, Result: %d", expected, consumed)
			}
		})
	}
}
