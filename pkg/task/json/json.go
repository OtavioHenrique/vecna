package json

import (
	"context"
	"encoding/json"
	"log/slog"
)

// JsonAdaptFn receives output from previous worker and need to return []bytes to be unmarshalled into result of JsonTargetFn
type JsonAdaptFn func(i interface{}, m map[string]interface{}) ([]byte, error)

// JsonTargetFn returns a new instance of the struct which the result from JsonAdaptFn will be unmarshalled
// Pay attention that the given struct need to have JSON meta annotations to work.
type JsonTargetFn func(m map[string]interface{}) (interface{}, error)

// JsonUnmarshaller is a generic task to unmarshall JSONs.
// It needs two functions to execute its jobs. One to adapt the result from previous worker, and one to return a target struct to be  unmarshalled.
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

// JsonMarshalAdaptFn receives the output from previous work and adapt it to the struct who JSON will be marshalled. Pay attention that the given struct need to have JSON meta annotations to work.
type JsonMarshalAdaptFn func(i interface{}, m map[string]interface{}) (interface{}, error)

// JsonMarshaller is a generic task which marshal any given struct into JSON. Pay attention that the given struct need to have JSON meta annotations to work.
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
