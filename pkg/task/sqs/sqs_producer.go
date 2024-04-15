package sqs

import (
	"context"
	"log/slog"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// SQS Producer  options
type SQSProducerOpts struct {
	// Delay which message will be delivered (if not given, will use default from queue)
	DelaySeconds *int64
	// QueueName
	QueueName string
}

type SQSProducerInput struct {
	Body   string
	MsgAtt map[string]*sqs.MessageAttributeValue
}

// Simple generic task to produce messages to SQS
type SQSProducer[I SQSProducerInput, O task.Nullable] struct {
	// SQS AWS client to be used
	client   sqsiface.SQSAPI
	logger   *slog.Logger
	opts     *SQSProducerOpts
	queueURL *string
}

func NewSQSProducer[I SQSProducerInput, O task.Nullable](client sqsiface.SQSAPI, logger *slog.Logger, opts *SQSProducerOpts) *SQSProducer[I, O] {
	p := new(SQSProducer[I, O])

	p.client = client
	p.logger = logger
	p.opts = opts
	p.queueURL = p.GetQueueURL()

	return p
}

func (p *SQSProducer[I, O]) GetQueueURL() *string {
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
func (c *SQSProducer[I, O]) Run(_ context.Context, i I, meta map[string]interface{}, name string) (O, error) {
	input := SQSProducerInput(i)

	_, err := c.client.SendMessage(&sqs.SendMessageInput{
		DelaySeconds:      c.opts.DelaySeconds,
		MessageAttributes: input.MsgAtt,
		MessageBody:       &input.Body,
		QueueUrl:          c.queueURL,
	})

	return O(task.Nullable{}), err
}
