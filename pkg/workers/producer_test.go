package workers_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockTaskProducer struct {
	CalledWith []string
	CallCount  int
	mu         sync.Mutex
}

func (t *MockTaskProducer) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*task.TaskData, error) {

	t.mu.Lock()
	t.CalledWith = append(t.CalledWith, string(input.([]byte)))
	t.CallCount += 1
	msg := []byte(fmt.Sprintf("Called count: %d", t.CallCount))
	t.mu.Unlock()

	return &task.TaskData{Data: msg, Metadata: meta}, nil
}

func TestProducerWorker_Start(t *testing.T) {
	type fields struct {
		name      string
		Output    chan *workers.WorkerData
		task      task.Task
		numWorker int
		logger    *slog.Logger
		Metrics   metrics.Metric
		ticker    time.Duration
	}
	tests := []struct {
		name              string
		fields            fields
		expectedCallCount int
	}{
		{"Producing messages to task output", fields{
			name:      "Test Producer Worker",
			Output:    make(chan *workers.WorkerData),
			task:      &MockTaskProducer{},
			numWorker: 3,
			logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
			Metrics:   metrics.NewMockMetrics(),
			ticker:    1 * time.Millisecond,
		}, 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := workers.NewProducerlWorker(
				tt.fields.name,
				tt.fields.Output,
				tt.fields.task,
				tt.fields.numWorker,
				tt.fields.logger,
				tt.fields.Metrics,
				tt.fields.ticker,
			)
			w.Start(context.TODO())

			var outputMsgs []*workers.WorkerData

			for i := 0; i < tt.expectedCallCount; i++ {
				msg := <-tt.fields.Output
				if len(outputMsgs) < tt.expectedCallCount {
					outputMsgs = append(outputMsgs, msg)
				}
			}

			if count := len(outputMsgs); count != tt.expectedCallCount {
				t.Errorf("Producer worker didn't produced enought messages. Expected %d, Result %d", tt.expectedCallCount, count)
			}
		})
	}
}
