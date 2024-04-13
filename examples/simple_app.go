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

type Printer[I []byte, O []byte] struct{}

func (p *Printer[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	fmt.Println(input)

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

	inputCh := make(chan *workers.WorkerData[[]*sqs.SQSConsumerOutput], 10)
	sqsConsumer.AddOutputCh(inputCh)

	breaker := workers.NewEventBreakerWorker[[]*sqs.SQSConsumerOutput, *sqs.SQSConsumerOutput](
		"break sqs messages",
		1,
		logger,
		metric,
	)

	breaker.AddInputCh(inputCh)

	msgsCh := make(chan *workers.WorkerData[*sqs.SQSConsumerOutput], 10)
	breaker.AddOutputCh(msgsCh)

	pathExtractor := workers.NewBiDirectionalWorker[*sqs.SQSConsumerOutput, string](
		"Path Extractor",
		&PathExtractor[*sqs.SQSConsumerOutput, string]{},
		1,
		logger,
		metric,
	)
	pathCh := make(chan *workers.WorkerData[string], 10)
	pathExtractor.AddInputCh(msgsCh)
	pathExtractor.AddOutputCh(pathCh)

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
	s3Downloader.AddInputCh(pathCh)
	s3DownOutputCh := make(chan *workers.WorkerData[*s3.S3DownloaderOutput], 5)
	s3Downloader.AddOutputCh(s3DownOutputCh)

	// Other way of create intermediate workers without burocracy
	rawContentCh := make(chan *workers.WorkerData[[]byte], 10)
	go func() {
		for {
			out := <-s3DownOutputCh

			rawContentCh <- &workers.WorkerData[[]byte]{Data: out.Data.Data, Metadata: out.Metadata}
		}
	}()

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
	decompressor.AddInputCh(rawContentCh)
	decompressedCh := make(chan *workers.WorkerData[[]byte], 5)
	decompressor.AddOutputCh(decompressedCh)

	businessLogic := workers.NewConsumerWorker(
		"Process Data",
		&Printer[[]byte, []byte]{},
		5,
		logger,
		metric,
	)
	businessLogic.AddInputCh(decompressedCh)

	ctx := context.TODO()

	sqsConsumer.Start(ctx)
	breaker.Start(ctx)
	pathExtractor.Start(ctx)
	s3Downloader.Start(ctx)
	decompressor.Start(ctx)
	businessLogic.Start(ctx)
}
