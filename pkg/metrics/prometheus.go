package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var enqueuedMsgs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "vecna",
	Name:      "enqueued_messages",
	Help:      "number of messages enqueued",
}, []string{"queue"})

var consumedMsg = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vecna",
	Name:      "worker_consumed_message",
	Help:      "consumed message by worker",
}, []string{"worker_name"})

var producedMsg = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vecna",
	Name:      "worker_produced_message",
	Help:      "produced message by worker",
}, []string{"worker_name"})

var taskError = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vecna",
	Name:      "task_execution_error",
	Help:      "error on task execution",
}, []string{"worker_name"})

var taskSuccess = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vecna",
	Name:      "task_execution_success",
	Help:      "success on task execution",
}, []string{"worker_name"})

var taskRun = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "vecna",
	Name:      "task_execution",
	Help:      "task executed by worker",
}, []string{"worker_name"})

var taskRuntime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "vecna",
	Name:      "task_execution_time_milliseconds",
	Help:      "Task execution time in milliseconds",
	Buckets:   []float64{1, 5, 10, 15, 20, 35, 50, 100, 200, 350, 500, 750, 1000, 2000},
}, []string{"worker_name"})

type VecnaMetrics struct {
	enqueuedMessages prometheus.GaugeVec
	consumedMessage  prometheus.CounterVec
	producedMessage  prometheus.CounterVec
	taskError        prometheus.CounterVec
	taskSuccess      prometheus.CounterVec
	taskRun          prometheus.CounterVec
	taskRuntime      prometheus.HistogramVec
}

func NewVecnaMetrics() *VecnaMetrics {
	metrics := new(VecnaMetrics)

	metrics.enqueuedMessages = *enqueuedMsgs
	metrics.consumedMessage = *consumedMsg
	metrics.producedMessage = *producedMsg
	metrics.taskError = *taskError
	metrics.taskSuccess = *taskSuccess
	metrics.taskRun = *taskRun
	metrics.taskRuntime = *taskRuntime

	return metrics
}

func (m *VecnaMetrics) EnqueuedMessages(msgsNum int, queueName string) {
	m.enqueuedMessages.WithLabelValues(queueName).Set(float64(msgsNum))
}

func (m *VecnaMetrics) ConsumedMessage(workerName string) {
	m.consumedMessage.WithLabelValues(workerName).Inc()
}

func (m *VecnaMetrics) ProducedMessage(workerName string) {
	m.producedMessage.WithLabelValues(workerName).Inc()
}

func (m *VecnaMetrics) TaskError(workerName string) {
	m.taskError.WithLabelValues(workerName).Inc()
}

func (m *VecnaMetrics) TaskSuccess(workerName string) {
	m.taskSuccess.WithLabelValues(workerName).Inc()
}

func (m *VecnaMetrics) TaskRun(workerName string) {
	m.taskRun.WithLabelValues(workerName).Inc()
}

func (m *VecnaMetrics) ObserveTaskExecutionTime(workerName string, start time.Time, end time.Time) {
	elapsed := start.Sub(end)

	m.taskRuntime.WithLabelValues(workerName).Observe(float64(elapsed.Milliseconds()))
}
