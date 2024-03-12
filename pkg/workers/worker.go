package workers

import "context"

// Worker is a simple generic interface, every worker must have a Start and Stop method
// T will always be input type
// K will always be output type
type Worker[T any, K any] interface {
	// Start must perform all necessary logic to start the goroutine pool and listen to channels
	Start(context.Context)
	// Stop must perform all necessary logic to shutdown this worker
	Stop(context.Context)
	Name() string
	Started() bool
	InputCh() chan *WorkerData[T]
	OutputCh() chan *WorkerData[K]
}

// Worker Data is the unit which workers produces as output and receives as input
type WorkerData[K any] struct {
	Data     K
	Metadata map[string]interface{}
}
