package task

import (
	"context"
)

// Task performs a simple action. Usually only one action.
type Task[I any, O any] interface {
	// Run will be called by workers (usually)
	// it will receive context, a input (usually output from previous task)
	// should always return a O and error
	Run(context.Context, I, map[string]interface{}, string) (O, error)
}
