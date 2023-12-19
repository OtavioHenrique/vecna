package task_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/otaviohenrique/vecna/pkg/task"
)

func readToString(rs io.ReadSeeker) (string, error) {
	_, err := rs.Seek(0, io.SeekStart)
	if err != nil {
		return "", err
	}

	buffer := make([]byte, 1024)
	n, err := rs.Read(buffer)
	if err != nil && err != io.EOF {
		return "", err
	}

	str := string(buffer[:n])

	return str, nil
}

type S3UploaderMock struct {
	s3iface.S3API
	CalledWith []s3.PutObjectInput
	WantErr    bool
}

func (u *S3UploaderMock) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	if u.WantErr == true {
		return nil, errors.New("error-on-put")
	}

	out := new(s3.PutObjectOutput)
	u.CalledWith = append(u.CalledWith, *input)

	return out, nil
}

func TestS3Uploader_Run(t *testing.T) {
	type fields struct {
		client     s3iface.S3API
		bucketName string
		adaptFn    func(interface{}, map[string]interface{}) (*task.S3UploaderInput, error)
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
		want    *string
		wantErr bool
	}{
		{"it correct upload the file based on given input", fields{
			client:     &S3UploaderMock{WantErr: false},
			bucketName: "any-bucket",
			adaptFn: func(i interface{}, m map[string]interface{}) (*task.S3UploaderInput, error) {
				data, _ := i.([]string)

				return &task.S3UploaderInput{Path: &data[0], Content: []byte(data[1])}, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []string{"path/to/obj", "any-data-to-upload"},
			meta:  map[string]interface{}{},
		}, nil, false},
		{"it correct return error when aws call returns error", fields{
			client:     &S3UploaderMock{WantErr: true},
			bucketName: "any-bucket",
			adaptFn: func(i interface{}, m map[string]interface{}) (*task.S3UploaderInput, error) {
				data, _ := i.([]string)

				return &task.S3UploaderInput{Path: &data[0], Content: []byte(data[1])}, nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []string{"path/to/obj", "any-data-to-upload"},
			meta:  map[string]interface{}{},
		}, nil, true},
		{"it correct return error when adaptFn returns error", fields{
			client:     &S3UploaderMock{WantErr: false},
			bucketName: "any-bucket",
			adaptFn: func(_ interface{}, _ map[string]interface{}) (*task.S3UploaderInput, error) {
				return nil, errors.New("error-adapt-fn")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []string{"path/to/obj", "any-data-to-upload"},
			meta:  map[string]interface{}{},
		}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := task.NewS3Uploader(
				tt.fields.client,
				tt.fields.bucketName,
				tt.fields.adaptFn,
				tt.fields.logger,
			)
			_, err := s.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("S3Uploader.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				mck := tt.fields.client.(*S3UploaderMock)
				input := tt.args.input.([]string)

				if *mck.CalledWith[0].Key != input[0] {
					t.Errorf("S3Uploader.Run() want to call upload with right key, got := %s want = %s", *mck.CalledWith[0].Key, input[1])
				}

				if str, _ := readToString(mck.CalledWith[0].Body); str != input[1] {
					t.Errorf("S3Uploader.Run() want to call upload with right content, got := %s want = %s", str, input[0])
				}
			}
		})
	}
}
