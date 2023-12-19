package task

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type SQSConsumerOutput struct {
	// Content of the message
	Content *string
	// It receipt handler
	ReceiptHandle string
}

type SQSConsumerOpts struct {
	// QueueName which Consumer will use to get QueueURL
	QueueName string
	// VisibilityTimeout to be used
	VisibilityTimeout int64
	// MaxNumberOfMessages to Get when called. Max 10 on normal queues (SQS API max)
	MaxNumberOfMessages int64
}

// SQS Consumer is a Task that when called, get messages from a SQS queue and return them.
// It return an array of SQSConsumerOutput and append messages receipt handlers on metadata
// Exclude messages based on receipt handler is user responsability
type SQSConsumer struct {
	// SQS AWS client to be used
	client   sqsiface.SQSAPI
	logger   *slog.Logger
	opts     *SQSConsumerOpts
	queueURL *string
}

func NewSQSConsumer(client sqsiface.SQSAPI, logger *slog.Logger, opts *SQSConsumerOpts) *SQSConsumer {
	c := new(SQSConsumer)

	c.client = client
	c.logger = logger
	c.opts = opts
	c.queueURL = c.GetQueueURL()

	return c
}

func (c *SQSConsumer) GetQueueURL() *string {
	urlResult, err := c.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(c.opts.QueueName),
	})

	if err != nil {
		c.logger.Error("can't get queue name", "error", err)

		os.Exit(1)
	}

	return urlResult.QueueUrl
}

func (c *SQSConsumer) maxNumberOfMessages() int64 {
	return c.opts.MaxNumberOfMessages
}

func (c *SQSConsumer) visibilityTimeout() int64 {
	return c.opts.VisibilityTimeout
}

// Run when called consume messages from SQS and return TaskData with Data containing an array of *SQSConsumerOutput
// It appends receipt handlers to metadata to be excluded later by user
func (c *SQSConsumer) Run(_ context.Context, _ interface{}, meta map[string]interface{}, name string) ([]*SQSConsumerOutput, error) {
	msgs, err := c.receiveMessages(c.queueURL)

	if err != nil {
		c.logger.Error("error receiving messages", "error", err)
		return nil, err
	}

	var messagesOutput []*SQSConsumerOutput
	var receiptsHandler []string

	for i := 0; i < len(msgs); i++ {
		resp := SQSConsumerOutput{Content: msgs[i].Body, ReceiptHandle: *msgs[i].ReceiptHandle}

		receiptsHandler = append(receiptsHandler, *msgs[i].ReceiptHandle)
		messagesOutput = append(messagesOutput, &resp)
	}

	meta[name] = map[string][]string{"receiptHandlers": receiptsHandler}
	return messagesOutput, nil
}

func (c *SQSConsumer) receiveMessages(queueUrl *string) ([]*sqs.Message, error) {
	msgResult, err := c.client.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            queueUrl,
		MaxNumberOfMessages: aws.Int64(c.maxNumberOfMessages()),
		VisibilityTimeout:   aws.Int64(c.visibilityTimeout()),
	})

	if err != nil {
		return nil, err
	}

	return msgResult.Messages, nil
}
