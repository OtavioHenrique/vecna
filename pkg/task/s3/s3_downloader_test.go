package s3_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"

	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/otaviohenrique/vecna/pkg/task/s3"
)

type DataInput struct {
	Path string
}

type S3TaskInput struct {
	Data DataInput
}

type mockS3Client struct {
	s3iface.S3API
	calledWith       []awsS3.GetObjectInput
	ExpectedResponse string
	BucketName       string
	WantErr          bool
}

func (m *mockS3Client) GetObject(input *awsS3.GetObjectInput) (*awsS3.GetObjectOutput, error) {
	if m.WantErr == true {
		return nil, errors.New("test-error-download-s3")
	}

	resp := new(awsS3.GetObjectOutput)

	m.calledWith = append(m.calledWith, *input)
	reader := bytes.NewReader([]byte(m.ExpectedResponse))
	resp.Body = io.NopCloser(reader)

	return resp, nil
}

func TestS3Downloader_Run(t *testing.T) {
	type fields struct {
		client     s3iface.S3API
		bucketName string
		logger     *slog.Logger
	}
	type args struct {
		in0   context.Context
		input string
		meta  map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"It correctly take path and downloads it", fields{
			client:     &mockS3Client{ExpectedResponse: "response", BucketName: "bucket", WantErr: false},
			bucketName: "bucket",
			logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: "path/to/file",
			meta:  map[string]interface{}{},
		}, "response", false},
		{"It correctly handles download error on download", fields{
			client:     &mockS3Client{ExpectedResponse: "response", BucketName: "bucket", WantErr: true},
			bucketName: "bucket",
			logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: "path/to/file",
			meta:  map[string]interface{}{},
		}, "response", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := s3.NewS3Downloader(
				tt.fields.client,
				tt.fields.bucketName,
				tt.fields.logger,
			)
			got, err := s.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.name)

			if (err != nil) != tt.wantErr {
				t.Errorf("S3Downloader.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && !reflect.DeepEqual(string(got.Data), tt.want) {
				t.Errorf("S3Downloader.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}
