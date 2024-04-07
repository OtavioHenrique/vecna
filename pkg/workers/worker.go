package workers

import "context"

// Worker is a simple generic interface, every worker must have a Start and Stop method
type Worker[T any, K any] interface {
	// Start must perform all necessary logic to start the goroutine pool and listen to channels
	Start(context.Context)
	// Stop must perform all necessary logic to shutdown this worker
	Stop(context.Context)
	Name() string
	Started() bool
	AddInputCh(chan *WorkerData[T])
	AddOutputCh(chan *WorkerData[K])
	InputCh() chan *WorkerData[T]
	OutputCh() chan *WorkerData[K]
}

// Worker Data is the unit which workers produces as output and receives as input
type WorkerData[K any] struct {
	Data     K
	Metadata map[string]interface{}
}
