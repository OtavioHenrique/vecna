package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/otaviohenrique/vecna/pkg/executor"
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

func main() {
	a := executor.ExecutorInput[[]byte]{
		Worker: workers.NewProducerWorker[[]byte](
			"test-producer",
			&MockTask[[]byte]{},
			2,
			slog.New(slog.NewTextHandler(os.Stdout, nil)),
			metrics.NewMockMetrics(),
			500*time.Millisecond,
		), QueueSize: 10}

	b := executor.ExecutorInput[[]byte]{
		Worker: workers.NewBiDirectionalWorker[[]byte](
			"test-bidirectional",
			&MockTask[[]byte]{},
			2,
			slog.New(slog.NewTextHandler(os.Stdout, nil)),
			metrics.NewMockMetrics(),
		), QueueSize: 10}

	c := executor.ExecutorInput[[]byte]{
		Worker: workers.NewConsumerWorker[[]byte](
			"test-consumer",
			&MockTask[[]byte]{},
			2,
			slog.New(slog.NewTextHandler(os.Stdout, nil)),
			metrics.NewMockMetrics(),
		), QueueSize: 10}

	//StartW(a.Worker, b.Worker, c.Worker)
	fmt.Println("CHEGUEI AQUI>>>")
	executor.NewExecutor(
		slog.New(slog.NewTextHandler(os.Stdout, nil)), a, b, c).StartWorkers(context.TODO(), AllowedTypes)
}

type AllowedTypes interface {
	[]byte | int64
}

func StartW(ws ...any) {
	for _, input := range ws {
		switch input.(type) {
		case *workers.BiDirectionalWorker[any]:
			w := input.(*workers.BiDirectionalWorker[any])
			w.Start(context.TODO())
		case *workers.ProducerWorker[any]:
			w := input.(*workers.ProducerWorker[any])

			w.Start(context.TODO())
		case *workers.ConsumerWorker[any]:
			w := input.(*workers.ConsumerWorker[any])

			w.Start(context.TODO())
		}
	}
}
