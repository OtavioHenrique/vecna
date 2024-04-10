package workers_test

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

func TestEventBreakerWorker_Start(t *testing.T) {
	type fields struct {
		name      string
		Input     chan *workers.WorkerData[[]string]
		Output    chan *workers.WorkerData[string]
		numWorker int
		logger    *slog.Logger
		metric    metrics.Metric
	}
	type args struct {
		ctx context.Context
	}

	tests := []struct {
		name   string
		fields fields
		args   args
		input  []string
	}{
		{"It correct breaks events based on breakFn", fields{
			name:      "test-worker",
			Input:     make(chan *workers.WorkerData[[]string], 10),
			Output:    make(chan *workers.WorkerData[string], 20),
			numWorker: 1,
			logger:    slog.New(slog.NewTextHandler(os.Stdout, nil)),
			metric:    metrics.NewMockMetrics(),
		}, args{ctx: context.TODO()}, []string{"a", "b", "c", "d"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := workers.NewEventBreakerWorker[[]string, string](
				tt.fields.name,
				tt.fields.numWorker,
				tt.fields.logger,
				tt.fields.metric,
			)

			w.Input = tt.fields.Input
			w.Output = tt.fields.Output

			w.Start(tt.args.ctx)

			tt.fields.Input <- &workers.WorkerData[[]string]{Data: tt.input, Metadata: nil}

			var result []string
			for i := 0; i < len(tt.input); i++ {
				value := <-tt.fields.Output

				result = append(result, value.Data)
			}

			if reflect.DeepEqual(result, tt.input) != true {
				t.Errorf("Should have been received all content from input on different messages. Want = %s, Received = %s", tt.input, result)
			}
		})
	}
}
