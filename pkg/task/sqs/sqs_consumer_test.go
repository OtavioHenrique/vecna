package sqs_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/otaviohenrique/vecna/pkg/task/sqs"
)

type MockSQS struct {
	sqsiface.SQSAPI
	QueueURL         string
	calledWith       []awsSqs.ReceiveMessageInput
	ExpectedResponse string
	ReceiptHandle    string
	receiveCount     int
	WantErr          bool
}

func (s *MockSQS) GetQueueUrl(input *awsSqs.GetQueueUrlInput) (*awsSqs.GetQueueUrlOutput, error) {
	output := new(awsSqs.GetQueueUrlOutput)
	output.QueueUrl = aws.String(*input.QueueName)

	return output, nil
}

func (s *MockSQS) ReceiveMessage(input *awsSqs.ReceiveMessageInput) (*awsSqs.ReceiveMessageOutput, error) {
	if s.WantErr == true {
		return nil, errors.New("test-err")
	}

	s.calledWith = append(s.calledWith, *input)

	s.receiveCount++

	output := new(awsSqs.ReceiveMessageOutput)
	msg := awsSqs.Message{Body: aws.String(s.ExpectedResponse), ReceiptHandle: aws.String(s.ReceiptHandle)}

	output.Messages = []*awsSqs.Message{&msg}
	return output, nil
}

func TestSQSConsumer_Run(t *testing.T) {
	type fields struct {
		client sqsiface.SQSAPI
		logger *slog.Logger
		opts   *sqs.SQSConsumerOpts
	}
	type args struct {
		in0  context.Context
		in1  interface{}
		meta map[string]interface{}
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		want         string
		wantMetadata map[string]interface{}
		wantErr      bool
	}{
		{"It correct returns expected message", fields{
			client: &MockSQS{QueueURL: "any-queue", ExpectedResponse: "message", ReceiptHandle: "receipt-handler", WantErr: false},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &sqs.SQSConsumerOpts{QueueName: "any-queue", VisibilityTimeout: 100, MaxNumberOfMessages: 100},
		}, args{
			in0:  context.TODO(),
			in1:  struct{}{},
			meta: map[string]interface{}{"hello": "ola"},
		}, "message", map[string]interface{}{"hello": "ola", "worker": map[string][]string{"receiptHandlers": {"receipt-handler"}}}, false},
		{"It correct returns error", fields{
			client: &MockSQS{QueueURL: "any-queue", ExpectedResponse: "message", WantErr: true},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &sqs.SQSConsumerOpts{QueueName: "any-queue", VisibilityTimeout: 100, MaxNumberOfMessages: 100},
		}, args{
			in0:  context.TODO(),
			in1:  struct{}{},
			meta: map[string]interface{}{"hello": "ola"},
		}, "message", map[string]interface{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := sqs.NewSQSConsumer(
				tt.fields.client,
				tt.fields.logger,
				tt.fields.opts,
			)

			got, err := c.Run(tt.args.in0, tt.args.in1, tt.args.meta, "worker")
			if (err != nil) != tt.wantErr {
				t.Errorf("SQSConsumer.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(*got.([]*sqs.SQSConsumerOutput)[0].Content, tt.want) {
				t.Errorf("SQSConsumer.Run() = %v, want %v", *got.([]*sqs.SQSConsumerOutput)[0].Content, tt.want)
			}

			if !tt.wantErr && !reflect.DeepEqual(tt.wantMetadata, tt.args.meta) {
				t.Errorf("SQSConsumer.Run() = %v, want %v", tt.args.meta, tt.wantMetadata)
			}
		})
	}
}
