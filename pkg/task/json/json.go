package json

import (
	"context"
	"encoding/json"
	"log/slog"
)

// JsonUnmarshaller is a generic task to unmarshall JSONs.
type JsonUnmarshaller[I []byte, O any] struct {
	logger *slog.Logger
}

func NewJsonUnmarshaller[I []byte, O any](logger *slog.Logger) *JsonUnmarshaller[I, O] {
	u := new(JsonUnmarshaller[I, O])

	u.logger = logger

	return u
}

func (u *JsonUnmarshaller[I, O]) Run(_ context.Context, input []byte, meta map[string]interface{}, _ string) (O, error) {
	var target O

	err := json.Unmarshal(input, &target)

	if err != nil {
		var nullTarget O

		return nullTarget, err
	}

	return target, nil
}

// JsonMarshaller is a generic task which marshal any given struct into JSON.
type JsonMarshaller[I any, O []byte] struct {
	logger *slog.Logger
}

func NewJsonMarshaller[I any, O []byte](logger *slog.Logger) *JsonMarshaller[I, O] {
	u := new(JsonMarshaller[I, O])

	u.logger = logger

	return u
}

func (u *JsonMarshaller[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	bytes, err := json.Marshal(input)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}
