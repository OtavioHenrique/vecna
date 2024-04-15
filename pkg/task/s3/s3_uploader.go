package s3

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

// S3Uploader is a task which will upload a given data on the given path (key) of one bucket
type S3Uploader[I *S3UploaderInput, O task.Nullable] struct {
	// S3 AWS client to be used
	client s3iface.S3API
	// Bucket name where all objects will be stored
	bucketName string
	logger     *slog.Logger
}

// S3UploaderInput is a envelope containing all the information needed to upload object to S3
// Should be returned by adaptFn
type S3UploaderInput struct {
	// Path (Key) to upload the object
	Path string
	// Bytes to be uploaded
	Content []byte
}

func NewS3Uploader[I *S3UploaderInput, O task.Nullable](client s3iface.S3API, bucketName string, logger *slog.Logger) *S3Uploader[I, O] {
	u := new(S3Uploader[I, O])

	u.client = client
	u.bucketName = bucketName
	u.logger = logger

	return u
}

// Run() will be called by worker and should return a pointer to TaskData.
// It doesn't merge nothing on metadata given and only return errors if any
func (s *S3Uploader[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	err := s.uploadObject(input)

	return O(task.Nullable{}), err
}

func (s *S3Uploader[T, K]) uploadObject(input *S3UploaderInput) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s.bucketName),
		Key:    aws.String(input.Path),
		Body:   aws.ReadSeekCloser(bytes.NewReader(input.Content)),
	})

	if err != nil {
		s.logger.Error("error uploading object", "error", err, "path", input.Path)
		return err
	}

	s.logger.Debug("object uploaded successfully", "path", input.Path)

	return nil
}
