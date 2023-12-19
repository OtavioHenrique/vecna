package task

import (
	"context"
)

// Task performs a simple action. Usually only one action.
type Task interface {
	// Run will be called by workers (usually)
	// it will receive context, a interface{} (usually output from previous worker) and the metadata map
	// should always return a interface{} to be cast later and error
	Run(context.Context, interface{}, map[string]interface{}, string) (interface{}, error)
}
