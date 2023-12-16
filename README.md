Vecna
=======

Vecna is a tiny library to build high concurrent application easily and focousing on business.

### How to use

To use just create your workers and tasks as you want.

```go
sqsProducer := workers.NewProducerWorker(
    "Event Created",
    task.NewSQSConsumer(client, logger, opts),
    10,
    logger,
    metric,
    time.NewTicker(500 * time.Millisecond)
)

s3Downloader := workers.NewBidirectionalWorker(
    "Download Data",
    task.NewS3Downloader(
        client, 
        "bucket", 
        func(i interface{}, _ map[string]interface{}) (*string, error) {
				task, _ := i.(SQSOutput)

				return &task.Data.Path, nil
		},
        logger,
        metric)
)

businessLogic := workers.NewConsumerWorker(
    "Process Data",
    NewSomeBusinessTask(),
    5,
    logger,
    metric
)

executor := executor.NewExecutor(
    []executor.ExecutorInput{
        {Worker: sqsProducer, QueueSize: 10},
        {Worker: s3Downloader, QueueSize: 10},
        {Worker: businessLogic, QueueSize: 10},
    },
    logger
)

// StartWorkers return a map with queues info to later be used by channel watcher if needed
queues := executor.StartWorkers(context.TODO())
```

In this example, a simple logic is being made, consume message from SQS, Download object from S3 (Based on SQS Consumer output), and process it with some busines logic (custom Task). 

The use of Executor is needed to Start workers, and later Stop() if wanted.

### How it works

Vecna is based on two units, `workers` and `tasks`. 

* `workers` are a pool of goroutines who interact with channels and execute `tasks`.
* `tasks` are objects capable of execute some computation.

Workers will usually listen and produce on given channels, and execute tasks based on them. 

Currently three workers types are provided (more to come):

* [Producer](pkg/workers/producer.go): Worker pool who only produces messages to a channel based on `Task` execution response
* [Consumer](pkg/workers/consumer.go): Worker pool who only consume for a channel and execute tasks.
* [BiDirecional](pkg/workers/bi_directional.go): Worker pool who consumes from a channel, execute tasks and produces output on other channel.

Some basic tasks are already provided (and welcome):

* [SQS Consumer](pkg/task/sqs_consumer.go) (to use with [SQS Deleter](pkg/task/sqs_deleter.go))
* [S3 Uploader](pkg/task/s3_uploader.go)
* [S3 Downloader](pkg/task/s3_downloader.go)

But you're heavy encouraged to code your business logic too.

## Monitoring

Vecna already come with a solid set of log needed to debug and monitor and with a basic interface `Metric` which recommended use is with prometheus. If you don't want to use metrics now, just use the `metrics.TODO` provided.

The `EnqueuedMessages()` function from `Metric` is useful to monitor how your workers is dealing with data flow, and if some workers need more or less goroutines. To use this metric, is recommended to use the `task.ChannelWatcher` object.

```go
watcher := task.NewChannelWatcher(
    queues,
    metric,
    time.NewTicker(10 * time.Second)
)

watcher.Start()
```

### Development

Currently in development:

* Data decompressor/compressor
* SQS Producer
* Kafka Consumer/Producer