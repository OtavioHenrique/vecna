<center><img src='https://i.imgur.com/BttdRu0.jpg' width='250'></center>


Vecna
=======

Vecna is a tiny library to build high concurrent applications easily and focusing on business. It already comes with a handful of general tasks created.

### How it works

Vecna is based on two units, `workers` and `tasks`. 

* `workers` are a pool of goroutines who interact with channels and execute `tasks`.
* `tasks` are objects capable of executing some computation.

Workers will usually listen and produce on given channels, and execute tasks based on them. 

Currently, three workers types are provided (more to come):

* [Producer](pkg/workers/producer.go): Worker pool who only produces messages to a channel based on `Task` execution response
* [Consumer](pkg/workers/consumer.go): Worker pool who only consume for a channel and execute tasks.
* [BiDirecional](pkg/workers/bi_directional.go): Worker pool who consumes from a channel, execute tasks and produces output on other channel.
* [EventBreaker](pkg/workers/event_breaker.go): Worker pool who consumes from a queue where results from previous worker are lists, breaks it in varios events to the next.

Some basic tasks are already provided (and welcome):

* [SQS Consumer](pkg/task/sqs_consumer.go) (to use with [SQS Deleter](pkg/task/sqs_deleter.go))
* [S3 Uploader](pkg/task/s3_uploader.go)
* [S3 Downloader](pkg/task/s3_downloader.go)
* [Decompressor (gzip/zstd)](pkg/task/decompressor.go)
* [Compressor (gzip/zstd)](pkg/task/compressor.go)
* [Json marshal/unmarshal](pkg/task/json.go)

But you're heavily encouraged to code your business logic too.

## Monitoring

Vecna already comes with a solid set of logs needed to debug and monitor and with a basic interface `Metric` which recommended for use is with Prometheus. If you don't want to use metrics now, just use the `metrics.TODO` provided.

You can use the `metrics.PromMetrics`, just instantiate using `metrics.NewPromMetrics()` and register each metric on your Prometheus registry.

```
vecnaMetrics := metrics.NewPromMetrics()
prometheus.NewRegistry().MustRegister(
    vecnaMetrics.EnqueuedMsgs,
    vecnaMetrics.ConsumedMsg
    ...
)
````

### How to use

To use just create your workers and tasks as you want. Check my examples on [examples folder](examples/).

```go
metric := &metrics.TODO{}
logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

sess, err := session.NewSession(&aws.Config{
    Region: aws.String("us-east-1"),
})

if err != nil {
    panic(err)
}

sqsClient := sqs.New(sess)

sqsConsumer := workers.NewProducerWorker(
    "Event Created",
    task.NewSQSConsumer(sqsClient, logger, &task.SQSConsumerOpts{
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

s3Client := s3.New(sess)

s3Downloader := workers.NewBiDirectionalWorker(
    "Download Data",
    task.NewS3Downloader(
        s3Client,
        "bucket",
        func(i interface{}, _ map[string]interface{}) (*string, error) {
            task, _ := i.(*task.SQSConsumerOutput)

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
    task.NewDecompressor(
        "gzip",
        func(i interface{}, _ map[string]interface{}) ([]byte, error) {
            task, _ := i.(*task.S3DownloaderOutput)

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
```

In this example, a simple logic is being made, consume message from SQS, Download object from S3 (Based on SQS Consumer output), and process it with some busines logic (custom Task). 

The use of Executor is needed to Start workers, and later Stop() if wanted.

## Creating your own tasks

To create your own task is simple, just follow the [Task interface](pkg/task/task.go), a simple `Run()`` method is needed.

```
Run(context.Context, interface{}, map[string]interface{}, string) (interface{}, error)
```

### Development

Currently in development:

* Accumulator
* Kafka Consumer/Producer
