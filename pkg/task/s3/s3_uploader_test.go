package s3_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"testing"

	awsS3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/otaviohenrique/vecna/pkg/task/s3"
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
	CalledWith []awsS3.PutObjectInput
	WantErr    bool
}

func (u *S3UploaderMock) PutObject(input *awsS3.PutObjectInput) (*awsS3.PutObjectOutput, error) {
	if u.WantErr == true {
		return nil, errors.New("error-on-put")
	}

	out := new(awsS3.PutObjectOutput)
	u.CalledWith = append(u.CalledWith, *input)

	return out, nil
}

func TestS3Uploader_Run(t *testing.T) {
	type fields struct {
		client     s3iface.S3API
		bucketName string
		logger     *slog.Logger
	}
	type args struct {
		in0   context.Context
		input s3.S3UploaderInput
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
			logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: s3.S3UploaderInput{Path: "path/to/obj", Content: []byte("any-data-to-upload")},
			meta:  map[string]interface{}{},
		}, nil, false},
		{"it correct return error when aws call returns error", fields{
			client:     &S3UploaderMock{WantErr: true},
			bucketName: "any-bucket",
			logger:     slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: s3.S3UploaderInput{Path: "path/to/obj", Content: []byte("any-data-to-upload")},
			meta:  map[string]interface{}{},
		}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := s3.NewS3Uploader(
				tt.fields.client,
				tt.fields.bucketName,
				tt.fields.logger,
			)

			_, err := s.Run(tt.args.in0, &tt.args.input, tt.args.meta, tt.name)

			if (err != nil) != tt.wantErr {
				t.Errorf("S3Uploader.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				mck := tt.fields.client.(*S3UploaderMock)
				input := tt.args.input

				if *mck.CalledWith[0].Key != input.Path {
					t.Errorf("S3Uploader.Run() want to call upload with right key, got := %s want = %s", *mck.CalledWith[0].Key, input.Path)
				}

				if str, _ := readToString(mck.CalledWith[0].Body); str != string(input.Content) {
					t.Errorf("S3Uploader.Run() want to call upload with right content, got := %s want = %s", str, string(input.Content))
				}
			}
		})
	}
}
