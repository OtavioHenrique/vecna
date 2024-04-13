package s3

import (
	"context"
	"io"
	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// S3Downloader is a generic task capable of download a object from AWS S3 based on a given path and bucket name
// it will receive the bucket name on the constructor function NewS3Downloader and the path must be returned by adaptFN
// adaptFn will be called with input on method Run()
type S3Downloader[I string, O *S3DownloaderOutput] struct {
	// S3 AWS client to be used
	client s3iface.S3API
	// Bucket name where all objects will be downloaded
	bucketName string
	logger     *slog.Logger
}

type S3DownloaderOutput struct {
	Data []byte
}

func NewS3Downloader[I string, O *S3DownloaderOutput](client s3iface.S3API, bucketName string, logger *slog.Logger) *S3Downloader[I, O] {
	s := new(S3Downloader[I, O])

	s.client = client
	s.bucketName = bucketName
	s.logger = logger

	return s
}

// The return from Run() will be a S3DownloaderOutput (containing object as []data) and Metadata
// No metadata will be added.
func (s *S3Downloader[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(string(input)),
	})

	if err != nil {
		s.logger.Error("error downloading object", "error", err, "path", input)
		return nil, err
	}

	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		s.logger.Error("error reading downloaded object", "error", err, "path", input)

		return nil, err
	}

	s.logger.Debug("object downloaded successfully", "path", input)

	return &S3DownloaderOutput{Data: body}, nil
}
