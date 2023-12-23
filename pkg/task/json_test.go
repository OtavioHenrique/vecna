package task_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/otaviohenrique/vecna/pkg/task"
)

type TestStructJson struct {
	Name string `json:"name"`
}

func TestJsonUnmarshaller_Run(t *testing.T) {
	type fields struct {
		jsonAdaptFn  task.JsonAdaptFn
		jsonTargetFn task.JsonTargetFn
		logger       *slog.Logger
	}
	type args struct {
		in0   context.Context
		input interface{}
		meta  map[string]interface{}
		in3   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"unmarshal json on struct returned by targetFn correctly", fields{
			jsonAdaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return []byte(i.(string)), nil
			},
			jsonTargetFn: func(m map[string]interface{}) (interface{}, error) {
				return &TestStructJson{}, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: "{\"name\": \"test-name\"}",
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, &TestStructJson{Name: "test-name"}, false},
		{"returns error when jsonAdaptFn returns", fields{
			jsonAdaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return nil, errors.New("error-json-adapt")
			},
			jsonTargetFn: func(m map[string]interface{}) (interface{}, error) {
				return &TestStructJson{}, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: "{\"name\": \"test-name\"}",
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, &TestStructJson{Name: "test-name"}, true},
		{"returns error when jsonTargetFn returns", fields{
			jsonAdaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return []byte(i.(string)), nil
			},
			jsonTargetFn: func(m map[string]interface{}) (interface{}, error) {
				return nil, errors.New("error-json-target")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: "{\"name\": \"test-name\"}",
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, &TestStructJson{Name: "test-name"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := task.NewJsonUnmarshaller(
				tt.fields.jsonAdaptFn,
				tt.fields.jsonTargetFn,
				tt.fields.logger,
			)
			got, err := u.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonUnmarshaller.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonUnmarshaller.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJsonMarshaller_Run(t *testing.T) {
	type fields struct {
		jsonMarshalAdaptFn task.JsonMarshalAdaptFn
		logger             *slog.Logger
	}
	type args struct {
		in0   context.Context
		input interface{}
		meta  map[string]interface{}
		in3   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"Marshal JSON correctly based on adaptFn result", fields{
			jsonMarshalAdaptFn: func(i interface{}, m map[string]interface{}) (interface{}, error) {
				return i.(*TestStructJson), nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: &TestStructJson{Name: "test-name"},
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, []byte("{\"name\":\"test-name\"}"), false},
		{"returns error when jsonMarshalAdaptFn returns", fields{
			jsonMarshalAdaptFn: func(i interface{}, m map[string]interface{}) (interface{}, error) {
				return nil, errors.New("error-json-marshal-adapt")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: &TestStructJson{Name: "test-name"},
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, []byte("{\"name\":\"test-name\"}"), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := task.NewJsonMarshaller(
				tt.fields.jsonMarshalAdaptFn,
				tt.fields.logger,
			)

			got, err := u.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonMarshaller.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonMarshaller.Run() = %v, want %v", string(got.([]byte)), string(tt.want.([]byte)))
			}
		})
	}
}
