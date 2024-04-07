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

type MockSQSProducer struct {
	sqsiface.SQSAPI
	QueueURL         string
	CalledWith       []awsSqs.SendMessageInput
	ExpectedResponse string
	ReceiptHandle    string
	receiveCount     int
	WantErr          bool
}

func (s *MockSQSProducer) GetQueueUrl(input *awsSqs.GetQueueUrlInput) (*awsSqs.GetQueueUrlOutput, error) {
	output := new(awsSqs.GetQueueUrlOutput)
	output.QueueUrl = aws.String(*input.QueueName)

	return output, nil
}

func (s *MockSQSProducer) SendMessage(input *awsSqs.SendMessageInput) (*awsSqs.SendMessageOutput, error) {
	if s.WantErr == true {
		return nil, errors.New("test-err")
	}

	s.CalledWith = append(s.CalledWith, *input)

	s.receiveCount++

	return nil, nil
}

func TestSQSProducer_Run(t *testing.T) {
	type fields struct {
		client sqsiface.SQSAPI
		logger *slog.Logger
		opts   *sqs.SQSProducerOpts
	}
	type args struct {
		in0  context.Context
		i    sqs.SQSProducerInput
		meta map[string]interface{}
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte //TODO nullable type
		wantErr bool
	}{
		{"It correctly sends message", fields{
			client: &MockSQSProducer{},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &sqs.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0: context.TODO(),
			i: sqs.SQSProducerInput{
				Body: "test-message",
			},
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, false},
		{"It correctly returns error SQS client returns while sending message", fields{
			client: &MockSQSProducer{WantErr: true},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &sqs.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0: context.TODO(),
			i: sqs.SQSProducerInput{
				Body: "test-message",
			},
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, true},
		{"It correctly sends metadata when metadata fn is  given", fields{
			client: &MockSQSProducer{},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
			opts:   &sqs.SQSProducerOpts{QueueName: "queue-name"},
		}, args{
			in0: context.TODO(),
			i: sqs.SQSProducerInput{
				Body: "test-message",
				MsgAtt: map[string]*awsSqs.MessageAttributeValue{
					"Meaning": {
						DataType:    aws.String("String"),
						StringValue: aws.String("42"),
					},
				},
			},
			meta: map[string]interface{}{},
			name: "test-worker",
		}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := sqs.NewSQSProducer(
				tt.fields.client,
				tt.fields.logger,
				tt.fields.opts,
			)

			_, err := c.Run(tt.args.in0, tt.args.i, tt.args.meta, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("SQSProducer.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(*tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageBody, tt.args.i.Body) {
				t.Errorf("Wrong message produced = %v, want %v", *tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageBody, tt.args.i.Body)
			}

			if !tt.wantErr && tt.args.i.MsgAtt != nil {
				producedMeta := tt.fields.client.(*MockSQSProducer).CalledWith[0].MessageAttributes

				if !reflect.DeepEqual(producedMeta, tt.args.i.MsgAtt) {
					t.Errorf("Wrong metadata field produced on message = %v, want %v", producedMeta, tt.args.i.MsgAtt)
				}
			}
		})
	}
}
