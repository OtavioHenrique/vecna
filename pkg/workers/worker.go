package workers

import "context"

// Worker is a simple generic interface which will execute task async, every worker must have a Start and Stop method
type Worker[I any, O any] interface {
	// Start must perform all necessary logic to start the goroutine pool and listen to channels
	Start(context.Context)
	// Stop must perform all necessary logic to shutdown this worker
	Stop(context.Context)
	Name() string
	Started() bool
	AddInputCh(chan *WorkerData[I])
	AddOutputCh(chan *WorkerData[O])
	InputCh() chan *WorkerData[I]
	OutputCh() chan *WorkerData[O]
}

// Worker Data is the unit which workers produces as output and receives as input
type WorkerData[K any] struct {
	Data     K
	Metadata map[string]interface{}
}
