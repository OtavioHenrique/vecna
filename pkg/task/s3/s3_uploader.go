package s3

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

// S3Uploader is a task which will upload a given data on the given path (key) of one bucket
type S3Uploader struct {
	// S3 AWS client to be used
	client s3iface.S3API
	// Bucket name where all objects will be stored
	bucketName string
	// adaptFn will be called on Run() with input and should return a pointer to S3UploaderInput
	adaptFn func(interface{}, map[string]interface{}) (*S3UploaderInput, error)
	logger  *slog.Logger
}

// S3UploaderInput is a envelope containing all the information needed to upload object to S3
// Should be returned by adaptFn
type S3UploaderInput struct {
	// Path (Key) to upload the object
	Path *string
	// Bytes to be uploaded
	Content []byte
}

func NewS3Uploader(client s3iface.S3API, bucketName string, adaptFn func(interface{}, map[string]interface{}) (*S3UploaderInput, error), logger *slog.Logger) *S3Uploader {
	u := new(S3Uploader)

	u.client = client
	u.bucketName = bucketName
	u.adaptFn = adaptFn
	u.logger = logger

	return u
}

// Run() will be called by worker and should return a pointer to TaskData.
// It doesn't merge nothing on metadata given and only return errors if any
func (s *S3Uploader) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	s3Input, err := s.adaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	_, err = s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(*s3Input.Path),
		Body:   aws.ReadSeekCloser(bytes.NewReader(s3Input.Content)),
	})

	if err != nil {
		s.logger.Error("error uploading object", "error", err, "path", s3Input.Path)
		return nil, err
	}

	s.logger.Debug("object uploaded successfully", "path", s3Input.Path)

	return nil, nil
}
