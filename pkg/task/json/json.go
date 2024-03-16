package json

import (
	"context"
	"encoding/json"
	"log/slog"
)

// JsonUnmarshaller is a generic task to unmarshall JSONs.
type JsonUnmarshaller[T []byte, K any] struct {
	logger *slog.Logger
}

func NewJsonUnmarshaller[T []byte, K any](logger *slog.Logger) *JsonUnmarshaller[T, K] {
	u := new(JsonUnmarshaller[T, K])

	u.logger = logger

	return u
}

func (u *JsonUnmarshaller[T, K]) Run(_ context.Context, input []byte, meta map[string]interface{}, _ string) (K, error) {
	var target K

	err := json.Unmarshal(input, &target)

	if err != nil {
		var nullTarget K

		return nullTarget, err
	}

	return target, nil
}

// JsonMarshaller is a generic task which marshal any given struct into JSON.
type JsonMarshaller[T any, K []byte] struct {
	logger *slog.Logger
}

func NewJsonMarshaller[T any, K []byte](logger *slog.Logger) *JsonMarshaller[T, K] {
	u := new(JsonMarshaller[T, K])

	u.logger = logger

	return u
}

func (u *JsonMarshaller[T, K]) Run(_ context.Context, input T, meta map[string]interface{}, _ string) (K, error) {
	bytes, err := json.Marshal(input)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}
