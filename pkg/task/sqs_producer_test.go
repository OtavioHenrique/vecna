package task_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

type MockSQSProducer struct {
	sqsiface.SQSAPI
	QueueURL         string
	CalledWith       []sqs.SendMessageInput
	ExpectedResponse string
	ReceiptHandle    string
	receiveCount     int
	WantErr          bool
}

func (s *MockSQSProducer) GetQueueUrl(input *sqs.GetQueueUrlInput) (*sqs.GetQueueUrlOutput, error) {
	output := new(sqs.GetQueueUrlOutput)
	output.QueueUrl = aws.String(*input.QueueName)

	return output, nil
}

func (s *MockSQSProducer) SendMessage(input *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	if s.WantErr == true {
		return nil, errors.New("test-err")
	}

	s.CalledWith = append(s.CalledWith, *input)

	s.receiveCount++

	return nil, nil
}

func TestSQSProducer_Run(t *testing.T) {
	type fields struct {
		client              sqsiface.SQSAPI
		sqsProducerAdaptFn  task.SqsProducerAdaptFn
		sqsProducerMsgAttfn task.SqsProducerMsgAttFn
		logger              *slog.Logger
		opts                *task.SQSProducerOpts
	}
	type args struct {
		in0  context.Context
		i    interface{}
		meta map[string]interface{}
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"It correctly sends message", fields{
			client: &MockSQSProducer{},
			sqsProducerAdaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				msg, _ := i.(string)
				return &msg, nil
			},
			sqsProducerMsgAttfn: nil,
			logger:              slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:                &task.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0:  context.TODO(),
			i:    "test-message",
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, false},
		{"It correctly returns error when adaptFn returns", fields{
			client: &MockSQSProducer{},
			sqsProducerAdaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				return nil, errors.New("adaptFn returned error")
			},
			sqsProducerMsgAttfn: nil,
			logger:              slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:                &task.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0:  context.TODO(),
			i:    "test-message",
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, true},
		{"It correctly returns error SQS client returns while sending message", fields{
			client: &MockSQSProducer{WantErr: true},
			sqsProducerAdaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				msg, _ := i.(string)
				return &msg, nil
			},
			sqsProducerMsgAttfn: nil,
			logger:              slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:                &task.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0:  context.TODO(),
			i:    "test-message",
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, true},
		{"It correctly sends metadata when metadata fn is  given", fields{
			client: &MockSQSProducer{},
			sqsProducerAdaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				msg, _ := i.(string)
				return &msg, nil
			},
			sqsProducerMsgAttfn: func(i interface{}, m map[string]interface{}) (map[string]*sqs.MessageAttributeValue, error) {
				messageAttributes := map[string]*sqs.MessageAttributeValue{
					"Meaning": {
						DataType:    aws.String("String"),
						StringValue: aws.String("42"),
					},
				}

				return messageAttributes, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &task.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0:  context.TODO(),
			i:    "test-message",
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, false},
		{"It correctly returns error when sqsProducerMsgAttfn returns error", fields{
			client: &MockSQSProducer{},
			sqsProducerAdaptFn: func(i interface{}, m map[string]interface{}) (*string, error) {
				msg, _ := i.(string)
				return &msg, nil
			},
			sqsProducerMsgAttfn: func(i interface{}, m map[string]interface{}) (map[string]*sqs.MessageAttributeValue, error) {
				return nil, errors.New("test-error-sqs-producer-msg-att-fn")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &task.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0:  context.TODO(),
			i:    "test-message",
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := task.NewSQSProducer(
				tt.fields.client,
				tt.fields.sqsProducerAdaptFn,
				tt.fields.sqsProducerMsgAttfn,
				tt.fields.logger,
				tt.fields.opts,
			)

			_, err := c.Run(tt.args.in0, tt.args.i, tt.args.meta, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("SQSProducer.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(*tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageBody, tt.args.i) {
				t.Errorf("Wrong message produced = %v, want %v", *tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageBody, tt.args.i)
			}

			if !tt.wantErr && tt.fields.sqsProducerMsgAttfn != nil {
				att, _ := tt.fields.sqsProducerMsgAttfn(tt.args.i, tt.args.meta)
				producedMeta := tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageAttributes

				if !reflect.DeepEqual(producedMeta, att) {
					t.Errorf("Wrong metadata field produced on message = %v, want %v", producedMeta, att)
				}
			}
		})
	}
}
