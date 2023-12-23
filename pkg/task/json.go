package task

import (
	"context"
	"encoding/json"
	"log/slog"
)

type JsonAdaptFn func(i interface{}, m map[string]interface{}) ([]byte, error)
type JsonTargetFn func(m map[string]interface{}) (interface{}, error)

type JsonUnmarshaller struct {
	jsonAdaptFn  JsonAdaptFn
	jsonTargetFn JsonTargetFn
	logger       *slog.Logger
}

func NewJsonUnmarshaller(adapt JsonAdaptFn, target JsonTargetFn, logger *slog.Logger) *JsonUnmarshaller {
	u := new(JsonUnmarshaller)

	u.jsonAdaptFn = adapt
	u.jsonTargetFn = target
	u.logger = logger
	return u
}

func (u *JsonUnmarshaller) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	bytes, err := u.jsonAdaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	target, err := u.jsonTargetFn(meta)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, target)

	if err != nil {
		return nil, err
	}

	return target, nil
}

type JsonMarshalAdaptFn func(i interface{}, m map[string]interface{}) (interface{}, error)

type JsonMarshaller struct {
	jsonMarshalAdaptFn JsonMarshalAdaptFn
	logger             *slog.Logger
}

func NewJsonMarshaller(adapt JsonMarshalAdaptFn, logger *slog.Logger) *JsonMarshaller {
	u := new(JsonMarshaller)

	u.jsonMarshalAdaptFn = adapt
	u.logger = logger

	return u
}

func (u *JsonMarshaller) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	strc, err := u.jsonMarshalAdaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal(strc)

	if err != nil {
		return nil, err
	}

	return bytes, nil
}
