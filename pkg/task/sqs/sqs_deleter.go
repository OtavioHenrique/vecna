package sqs

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
type SQSDeleter[T string, K []byte] struct {
	// SQS AWS Client to be used
	client   sqsiface.SQSAPI
	logger   *slog.Logger
	opts     *SQSDeleterOpts
	queueURL *string
}

func NewSQSDeleter[T string, K []byte](client sqsiface.SQSAPI, logger *slog.Logger, opts *SQSDeleterOpts) *SQSDeleter[T, K] {
	c := new(SQSDeleter[T, K])

	c.client = client
	c.logger = logger
	c.opts = opts
	c.queueURL = c.GetQueueURL()

	return c
}

func (s *SQSDeleter[T, K]) GetQueueURL() *string {
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
func (s *SQSDeleter[T, K]) Run(_ context.Context, input T, meta map[string]interface{}, _ string) (K, error) {
	_, err := s.deleteMessage(string(input))

	if err != nil {
		return nil, err
	}

	s.logger.Debug("sqs message deleted", "receiptHandle", string(input))

	return nil, nil
}

func (s *SQSDeleter[T, K]) deleteMessage(receipt string) (*sqs.DeleteMessageOutput, error) {
	resp, err := s.client.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(*s.queueURL),
		ReceiptHandle: aws.String(receipt),
	})

	return resp, err
}
