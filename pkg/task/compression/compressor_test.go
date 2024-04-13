package compression_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/otaviohenrique/vecna/pkg/task/compression"
)

func CompressGzip2(s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(s))
	w.Close()

	return b.Bytes()
}

func CompressZstd2(s string) []byte {
	var b io.Writer
	w, _ := zstd.NewWriter(b)

	return w.EncodeAll([]byte(s), []byte{})
}

func TestCompressor_Run(t *testing.T) {
	type fields struct {
		compressionType string
		logger          *slog.Logger
	}
	type args struct {
		in0   context.Context
		input []byte
		meta  map[string]interface{}
		in3   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []byte
		wantErr bool
	}{
		{"It correct compress gzip", fields{
			compressionType: "gzip",
			logger:          slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []byte("hello-world"),
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, CompressGzip2("hello-world"), false},
		{"It correct compress zstd", fields{
			compressionType: "zstd",
			logger:          slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: []byte("hello-world"),
			meta:  map[string]interface{}{},
			in3:   "test-worker-name",
		}, CompressZstd2("hello-world"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := compression.NewCompressor(
				tt.fields.compressionType,
				tt.fields.logger,
			)
			got, err := d.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("Compressor.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Compressor.Run() = %v, want %v", got, tt.want)
			}
		})
	}
}
