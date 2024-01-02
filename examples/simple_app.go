package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/otaviohenrique/vecna/pkg/executor"
	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task/compression"
	"github.com/otaviohenrique/vecna/pkg/task/s3"
	"github.com/otaviohenrique/vecna/pkg/task/sqs"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type Printer struct{}

func (p *Printer) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	data := input.([]byte)

	fmt.Println(data)

	return nil, nil
}

func main() {
	metric := &metrics.TODO{}
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	})

	if err != nil {
		panic(err)
	}

	sqsClient := awsSqs.New(sess)

	sqsConsumer := workers.NewProducerWorker(
		"Event Created",
		sqs.NewSQSConsumer(sqsClient, logger, &sqs.SQSConsumerOpts{
			QueueName: "any-queue",
		}),
		10,
		logger,
		metric,
		500*time.Millisecond,
	)

	breaker := workers.NewEventBreakerWorker(
		"break sqs messages",
		1,
		logger,
		metric,
	)

	s3Client := awsS3.New(sess)

	s3Downloader := workers.NewBiDirectionalWorker(
		"Download Data",
		s3.NewS3Downloader(
			s3Client,
			"bucket",
			func(i interface{}, _ map[string]interface{}) (*string, error) {
				task, _ := i.(*sqs.SQSConsumerOutput)

				return task.Content, nil
			},
			logger,
		),
		5,
		logger,
		metric,
	)

	decompressor := workers.NewBiDirectionalWorker(
		"Decompress Data",
		compression.NewDecompressor(
			"gzip",
			func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				task, _ := i.(*s3.S3DownloaderOutput)

				return task.Data, nil
			},
			logger,
		),
		5,
		logger,
		metric,
	)

	businessLogic := workers.NewConsumerWorker(
		"Process Data",
		&Printer{},
		5,
		logger,
		metric,
	)

	executor := executor.NewExecutor(
		[]executor.ExecutorInput{
			{Worker: sqsConsumer, QueueSize: 10},
			{Worker: breaker, QueueSize: 10},
			{Worker: s3Downloader, QueueSize: 10},
			{Worker: decompressor, QueueSize: 10},
			{Worker: businessLogic, QueueSize: 10},
		},
		logger,
	)

	executor.StartWorkers(context.TODO())
}
