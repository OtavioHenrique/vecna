package sqs

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// SQS Producer  options
type SQSProducerOpts struct {
	// Delay which message will be delivered (if not given, will use default from queue)
	DelaySeconds *int64
	// QueueName
	QueueName string
}

// Function which will receive output from previous worker and metadata, and return the message string to be sent to SQS
type SqsProducerAdaptFn func(interface{}, map[string]interface{}) (*string, error)

// Optional SqsProducerMsgAttFn will return a map of string (name) and MessageAttributeValues to be used as SQS message attribute if wanted.
// Ref https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-message-metadata.html
type SqsProducerMsgAttFn func(interface{}, map[string]interface{}) (map[string]*sqs.MessageAttributeValue, error)

// Simple generic task to produce messages to SQS
type SQSProducer struct {
	// SQS AWS client to be used
	client              sqsiface.SQSAPI
	sqsProducerAdaptFn  SqsProducerAdaptFn
	sqsProducerMsgAttfn SqsProducerMsgAttFn
	logger              *slog.Logger
	opts                *SQSProducerOpts
	queueURL            *string
}

func NewSQSProducer(client sqsiface.SQSAPI, adaptFn SqsProducerAdaptFn, msgAttFn SqsProducerMsgAttFn, logger *slog.Logger, opts *SQSProducerOpts) *SQSProducer {
	p := new(SQSProducer)

	p.client = client
	p.sqsProducerAdaptFn = adaptFn
	p.sqsProducerMsgAttfn = msgAttFn
	p.logger = p.logger
	p.opts = opts
	p.queueURL = p.GetQueueURL()

	return p
}

func (p *SQSProducer) GetQueueURL() *string {
	urlResult, err := p.client.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(p.opts.QueueName),
	})

	if err != nil {
		p.logger.Error("can't get queue name", "error", err)

		os.Exit(1)
	}

	return urlResult.QueueUrl
}

// Run will produce message returned by sqsProducerAdaptFn to the targete SQS queue. It always returns nil, being capable of only return error if any happen
func (c *SQSProducer) Run(_ context.Context, i interface{}, meta map[string]interface{}, name string) (interface{}, error) {
	msg, err := c.sqsProducerAdaptFn(i, meta)

	if err != nil {
		return nil, err
	}

	var msgAttributes map[string]*sqs.MessageAttributeValue

	if c.sqsProducerMsgAttfn != nil {
		resp, err := c.sqsProducerMsgAttfn(i, meta)

		if err != nil {
			return nil, err
		}

		msgAttributes = resp
	} else {
		msgAttributes = nil
	}

	_, err = c.client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      c.opts.DelaySeconds,
		MessageAttributes: msgAttributes,
		MessageBody:       msg,
		QueueUrl:          c.queueURL,
	})

	return nil, err
}
