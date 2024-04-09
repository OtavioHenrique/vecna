package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	awsSqs "github.com/aws/aws-sdk-go/service/sqs"
	"github.com/otaviohenrique/vecna/pkg/metrics"
	"github.com/otaviohenrique/vecna/pkg/task/compression"
	"github.com/otaviohenrique/vecna/pkg/task/s3"
	"github.com/otaviohenrique/vecna/pkg/task/sqs"
	"github.com/otaviohenrique/vecna/pkg/workers"
)

type MockSQSMessage struct {
	Path string `json:"path"`
}

type PathExtractor[T *sqs.SQSConsumerOutput, K string] struct{}

func (p *PathExtractor[T, K]) Run(_ context.Context, in T, meta map[string]interface{}, name string) (K, error) {
	input := sqs.SQSConsumerOutput(*in)

	var msg MockSQSMessage
	err := json.Unmarshal([]byte(*input.Content), &msg)

	if err != nil {
		return "", err
	}

	return K(msg.Path), nil
}

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

	breaker := workers.NewEventBreakerWorker[[]*sqs.SQSConsumerOutput, *sqs.SQSConsumerOutput](
		"break sqs messages",
		1,
		logger,
		metric,
	)

	pathExtractor := workers.NewBiDirectionalWorker[*sqs.SQSConsumerOutput, string](
		"Path Extractor",
		&PathExtractor[*sqs.SQSConsumerOutput, string]{},
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

	ctx := context.TODO()

	sqsConsumer.Start(ctx)
	breaker.Start(ctx)
	pathExtractor.Start(ctx)
	s3Downloader.Start(ctx)
	decompressor.Start(ctx)
	businessLogic.Start(ctx)
}
