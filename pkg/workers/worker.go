package workers

import "context"

// Worker is a simple interface, every worker must have a Start and Stop method
type Worker[T any] interface {
	// Start must perform all necessary logic to start the goroutine pool and listen to channels
	Start(context.Context)
	// Stop must perform all necessary logic to shutdown this worker
	Stop(context.Context)
	Name() string
	Started() bool
	OutputCh() chan *WorkerData[T]
	InputCh() chan *WorkerData[T]
}

// Worker Data is the unit which workers produces as output and receives as input
type WorkerData[T any] struct {
	Data     T
	Metadata map[string]interface{}
}
