package compression_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/otaviohenrique/vecna/pkg/task/compression"
)

func CompressGzip(s string) []byte {
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	w.Write([]byte(s))
	w.Close()

	return b.Bytes()
}

func CompressZstd(s string) []byte {
	var b io.Writer
	w, _ := zstd.NewWriter(b)

	return w.EncodeAll([]byte(s), []byte{})
}

func TestDecompressor_Run(t *testing.T) {
	type fields struct {
		compressionType string
		adaptFn         compression.DecompressAdaptFn
		logger          *slog.Logger
	}
	type args struct {
		in0   context.Context
		input interface{}
		meta  map[string]interface{}
		in3   string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"It decompress Gzip correctly", fields{
			compressionType: "gzip",
			adaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return i.([]byte), nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: CompressGzip("test-decompression-gzip"),
			meta:  map[string]interface{}{},
			in3:   "test"},
			"test-decompression-gzip", false,
		},
		{"It decompress zstd correctly", fields{
			compressionType: "zstd",
			adaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return i.([]byte), nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: CompressZstd("test-decompression-zstd"),
			meta:  map[string]interface{}{},
			in3:   "test"},
			"test-decompression-zstd", false,
		},
		{"It returns adaptFN error correctly", fields{
			compressionType: "gzip",
			adaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return nil, errors.New("test-error")
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: nil,
			meta:  map[string]interface{}{},
			in3:   "test"},
			nil, true,
		},
		{"It returns error when compression type is unknown", fields{
			compressionType: "unknown compression type",
			adaptFn: func(i interface{}, _ map[string]interface{}) ([]byte, error) {
				return i.([]byte), nil
			},
			logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
		}, args{
			in0:   context.TODO(),
			input: CompressGzip("test-decompression-gzip"),
			meta:  map[string]interface{}{},
			in3:   "test"},
			nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := compression.NewDecompressor(
				tt.fields.compressionType,
				tt.fields.adaptFn,
				tt.fields.logger,
			)
			got, err := d.Run(tt.args.in0, tt.args.input, tt.args.meta, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("Decompressor.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(string(got.([]byte)), tt.want) {
				t.Errorf("Decompressor.Run() = %v, want %v", string(got.([]byte)), tt.want)
			}
		})
	}
}
