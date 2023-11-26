package task_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

type DataInput struct {
	Path string
}

type S3TaskInput struct {
	Data DataInput
}

type mockS3Client struct {
	s3iface.S3API
	calledWith       []s3.GetObjectInput
	ExpectedResponse string
	BucketName       string
	WantErr          bool
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if m.WantErr == true {
		return nil, errors.New("test-error-download-s3")
	}

	resp := new(s3.GetObjectOutput)

	m.calledWith = append(m.calledWith, *input)
	reader := bytes.NewReader([]byte(m.ExpectedResponse))
	resp.Body = io.NopCloser(reader)

	return resp, nil
}

func TestS3Downloader_Run(t *testing.T) {
	type fields struct {
		client     s3iface.S3API
		bucketName string
		adaptFn    func(interface{}, map[string]interface{}) (*string, error)
		logger     *slog.Logger
	}
	type args struct {
		in0   context.Context
		input interface{}
		meta  map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *task.TaskData
		wantErr bool
	}{
		{"It correctly take path and downloads it", fields{
			client:     &mockS3Client{ExpectedResponse: "response", BucketName: "bucket", WantErr: false},
			bucketName: "bucket",
			adaptFn: func(i interface{}, _ map[string]interface{}) (*string, error) {
				task, _ := i.(S3TaskInput)

				return &task.Data.Path, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: S3TaskInput{DataInput{Path: "path/to/file"}},
			meta:  map[string]interface{}{},
		}, &task.TaskData{Data: "response", Metadata: map[string]interface{}{}}, false},
		{"It correctly handles download error on download", fields{
			client:     &mockS3Client{ExpectedResponse: "response", BucketName: "bucket", WantErr: true},
			bucketName: "bucket",
			adaptFn: func(i interface{}, _ map[string]interface{}) (*string, error) {
				task, _ := i.(S3TaskInput)

				return &task.Data.Path, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: S3TaskInput{DataInput{Path: "path/to/file"}},
			meta:  map[string]interface{}{},
		}, &task.TaskData{Data: "response", Metadata: map[string]interface{}{}}, true},
		{"It correctly handles error on adaptFn", fields{
			client:     &mockS3Client{ExpectedResponse: "response", BucketName: "bucket", WantErr: false},
			bucketName: "bucket",
			adaptFn: func(i interface{}, _ map[string]interface{}) (*string, error) {
				return nil, errors.New("error-on-adapt-fn")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: S3TaskInput{DataInput{Path: "path/to/file"}},
			meta:  map[string]interface{}{},
		}, &task.TaskData{Data: "response", Metadata: map[string]interface{}{}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := task.NewS3Downloader(
				tt.fields.client,
				tt.fields.bucketName,
				tt.fields.adaptFn,
				tt.fields.logger,
			)
			got, err := s.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3Downloader.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(string(got.Data.(*task.S3DownloaderOutput).Data), tt.want.Data) {
				t.Errorf("S3Downloader.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}
