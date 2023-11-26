package task

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
type S3Downloader struct {
	// S3 AWS client to be used
	client s3iface.S3API
	// Bucket name where all objects will be downloaded
	bucketName string
	// AdaptFn which will return the path to the object as *string
	adaptFn func(interface{}, map[string]interface{}) (*string, error)
	logger  *slog.Logger
}

type S3DownloaderOutput struct {
	Data []byte
}

func NewS3Downloader(client s3iface.S3API, bucketName string, adaptFn func(interface{}, map[string]interface{}) (*string, error), logger *slog.Logger) *S3Downloader {
	s := new(S3Downloader)

	s.client = client
	s.bucketName = bucketName
	s.adaptFn = adaptFn
	s.logger = logger

	return s
}

// The return from Run() will be a S3DownloaderOutput (containing object as []data) and Metadata
// No metadata will be added.
func (s *S3Downloader) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (*TaskData, error) {
	path, err := s.adaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	result, err := s.client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(*path),
	})

	if err != nil {
		s.logger.Error("error downloading object", "error", err, "path", *path)
		return nil, err
	}

	body, err := io.ReadAll(result.Body)
	if err != nil {
		s.logger.Error("error reading downloaded object", "error", err, "path", *path)

		return nil, err
	}

	s.logger.Debug("object downloaded successfully", "path", *path)

	return &TaskData{Data: &S3DownloaderOutput{Data: body}, Metadata: meta}, nil
}
