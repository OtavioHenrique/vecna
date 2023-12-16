package task

import (
	"context"
)

// TaskData is produced by every task as output.
type TaskData[T any] struct {
	// Data output of the task, type interface{} must be cast by the user on adaptersFn
	Data T
	// Metadata which will be passed to the next tasks/workers. Always append, never create newer unless you really need it.
	Metadata map[string]interface{}
}

// Task performs a simple action. Usually only one action.
type Task[T any] interface {
	// Run will be called by workers (usually)
	// it will receive context, a interface{} (usually output from previous worker) and the metadata map
	// should always return a pointer to TaskData and error
	Run(context.Context, interface{}, map[string]interface{}, string) (*TaskData[any], error)
}
