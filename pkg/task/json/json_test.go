package json_test

import (
	"context"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/otaviohenrique/vecna/pkg/task/json"
)

type TestStructJson struct {
	Name string `json:"name"`
}

func TestJsonUnmarshaller_Run(t *testing.T) {
	type fields struct {
		logger *slog.Logger
	}
	type args struct {
		in0   context.Context
		input []byte
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
		{"unmarshal json on struct correctly", fields{
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []byte("{\"name\": \"test-name\"}"),
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, &TestStructJson{Name: "test-name"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := json.NewJsonUnmarshaller[[]byte, *TestStructJson](
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
		logger *slog.Logger
	}
	type args struct {
		in0   context.Context
		input *TestStructJson
		meta  map[string]interface{}
		in3   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"Marshal JSON correctly", fields{
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: &TestStructJson{Name: "test-name"},
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, []byte("{\"name\":\"test-name\"}"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u := json.NewJsonMarshaller[*TestStructJson, []byte](
				tt.fields.logger,
			)

			got, err := u.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("JsonMarshaller.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("JsonMarshaller.Run() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}
