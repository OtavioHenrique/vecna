package workers

import "context"

// Worker is a simple interface, every worker must have a Start and Stop method
type Worker interface {
	// Start must perform all necessary logic to start the goroutine pool and listen to channels
	Start(context.Context)
	// Stop must perform all necessary logic to shutdown this worker
	Stop(context.Context)
	Started() bool
	OutputCh() chan *WorkerData
	InputCh() chan *WorkerData
}

// Worker Data is the unit which workers produces as output and receives as input
type WorkerData struct {
	Data     interface{}
	Metadata map[string]interface{}
}
