package task_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

type MockSQSDeleter struct {
	sqsiface.SQSAPI
	CalledWith []sqs.DeleteMessageInput
	WantErr    bool
}

func (s *MockSQSDeleter) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	output := new(sqs.GetQueueUrlOutput)
	output.QueueUrl = aws.String(*input.QueueName)

	return output, nil
}

func (s *MockSQSDeleter) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	if s.WantErr == true {
		return nil, errors.New("delete-message-error")
	}

	s.CalledWith = append(s.CalledWith, *input)

	return &sqs.DeleteMessageOutput{}, nil
}

func TestSQSDeleter_Run(t *testing.T) {
	type fields struct {
		client  sqsiface.SQSAPI
		logger  *slog.Logger
		adaptFn func(interface{}, map[string]interface{}) (*string, error)
		opts    *task.SQSDeleterOpts
	}
	type args struct {
		in0   context.Context
		input interface{}
		meta  map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"It correctly deletes messages with received handler", fields{
			client: &MockSQSDeleter{WantErr: false},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			adaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				str, _ := i.(string)

				return &str, nil
			},
			opts: &task.SQSDeleterOpts{QueueName: "queue-name"},
		}, args{
			in0:   context.TODO(),
			input: "message-handler",
			meta:  map[string]interface{}{},
		}, false,
		},
		{"It correctly return error when an error is returned by delete call", fields{
			client: &MockSQSDeleter{WantErr: true},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			adaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				str, _ := i.(string)

				return &str, nil
			},
			opts: &task.SQSDeleterOpts{QueueName: "queue-name"},
		}, args{
			in0:   context.TODO(),
			input: "message-handler",
			meta:  map[string]interface{}{},
		}, true,
		},
		{"It correctly return error when an error returned by adaptFn", fields{
			client: &MockSQSDeleter{WantErr: true},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			adaptFn: func(_ interface{}, _ map[string]interface{}) (*string, error) {
				return nil, errors.New("error-on-adaptfn")
			},
			opts: &task.SQSDeleterOpts{QueueName: "queue-name"},
		}, args{
			in0:   context.TODO(),
			input: "message-handler",
			meta:  map[string]interface{}{},
		}, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := task.NewSQSDeleter(
				tt.fields.client,
				tt.fields.logger,
				tt.fields.adaptFn,
				tt.fields.opts,
			)
			got, err := s.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.name)

			if (err != nil) != tt.wantErr {
				t.Errorf("SQSDeleter.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && got != nil {
				t.Errorf("SQSDeleter.Run() = %v, want %v", got, nil)
			}
		})
	}
}
