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

type PromMetrics struct {
	EnqueuedMsgs prometheus.GaugeVec
	ConsumedMsg  prometheus.CounterVec
	ProducedMsg  prometheus.CounterVec
	TaskErr      prometheus.CounterVec
	TaskSucc     prometheus.CounterVec
	TaskR        prometheus.CounterVec
	TaskRT       prometheus.HistogramVec
}

func NewPromMetrics() *PromMetrics {
	metrics := new(PromMetrics)

	metrics.EnqueuedMsgs = *enqueuedMsgs
	metrics.ConsumedMsg = *consumedMsg
	metrics.ProducedMsg = *producedMsg
	metrics.TaskErr = *taskError
	metrics.TaskSucc = *taskSuccess
	metrics.TaskR = *taskRun
	metrics.TaskRT = *taskRuntime

	return metrics
}

func (m *PromMetrics) EnqueuedMessages(msgsNum int, queueName string) {
	m.EnqueuedMsgs.WithLabelValues(queueName).Set(float64(msgsNum))
}

func (m *PromMetrics) ConsumedMessage(workerName string) {
	m.ConsumedMsg.WithLabelValues(workerName).Inc()
}

func (m *PromMetrics) ProducedMessage(workerName string) {
	m.ProducedMsg.WithLabelValues(workerName).Inc()
}

func (m *PromMetrics) TaskError(workerName string) {
	m.TaskErr.WithLabelValues(workerName).Inc()
}

func (m *PromMetrics) TaskSuccess(workerName string) {
	m.TaskSucc.WithLabelValues(workerName).Inc()
}

func (m *PromMetrics) TaskRun(workerName string) {
	m.TaskR.WithLabelValues(workerName).Inc()
}

func (m *PromMetrics) TaskExecutionTime(workerName string, start time.Time, end time.Time) {
	elapsed := start.Sub(end)

	m.TaskRT.WithLabelValues(workerName).Observe(float64(elapsed.Milliseconds()))
}
