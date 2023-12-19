package task

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type SQSDeleterOpts struct {
	// SQS queue name which Deleter will use to get queue url
	QueueName string
}

// SQSDeleter will delete a message on SQS based on a given receiptHandle
type SQSDeleter struct {
	// SQS AWS Client to be used
	client sqsiface.SQSAPI
	logger *slog.Logger
	// adaptFn will be called by Run() with given input and metadata, should return a receiptHandle to be deleted
	adaptFn  func(interface{}, map[string]interface{}) (*string, error)
	opts     *SQSDeleterOpts
	queueURL *string
}

func NewSQSDeleter(client sqsiface.SQSAPI, logger *slog.Logger, adaptFn func(interface{}, map[string]interface{}) (*string, error), opts *SQSDeleterOpts) *SQSDeleter {
	c := new(SQSDeleter)

	c.client = client
	c.logger = logger
	c.adaptFn = adaptFn
	c.opts = opts
	c.queueURL = c.GetQueueURL()

	return c
}

func (s *SQSDeleter) GetQueueURL() *string {
	urlResult, err := s.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(s.opts.QueueName),
	})

	if err != nil {
		s.logger.Error("can't get queue name", "error", err)

		os.Exit(1)
	}

	return urlResult.QueueUrl
}

// Run() delete a message on SQS based on the return of adaptFn. It only returns errors
func (s *SQSDeleter) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*string, error) {
	handler, err := s.adaptFn(input, meta)
	if err != nil {
		return nil, err
	}

	_, err = s.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(*s.queueURL),
		ReceiptHandle: aws.String(*handler),
	})

	if err != nil {
		return nil, err
	}

	s.logger.Debug("sqs message deleted", "receiptHandle", handler)

	return nil, nil
}
