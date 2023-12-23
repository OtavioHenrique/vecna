package task

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"

	"github.com/klauspost/compress/gzip"

	"github.com/klauspost/compress/zstd"
)

type CompressAdaptFn func(interface{}, map[string]interface{}) ([]byte, error)

type Compressor struct {
	compressionType string
	adaptFn         CompressAdaptFn
	logger          *slog.Logger
}

func NewWriter(compressionType string, writer io.Writer) (io.WriteCloser, error) {
	var compressor io.WriteCloser
	var err error
	switch compressionType {
	case "gzip":
		compressor = gzip.NewWriter(writer)
	case "zstd":
		compressor, err = zstd.NewWriter(writer)
	default:
		return nil, errors.New("unknown-compression-type")
	}

	if err != nil {
		return nil, err
	}

	return compressor, nil
}

func NewCompressor(compressionType string, adaptFn CompressAdaptFn, logger *slog.Logger) *Compressor {
	d := new(Compressor)

	d.compressionType = compressionType
	d.adaptFn = adaptFn
	d.logger = logger

	return d
}

func (d *Compressor) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	b, err := d.adaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	zw, err := NewWriter(d.compressionType, &buf)
	if err != nil {
		return nil, err
	}

	_, err = zw.Write(b)
	if err != nil {
		return nil, err
	}

	zw.Close()

	result := buf.Bytes()
	os.WriteFile("/tmp/dat5", result, 0644)

	return result, nil
}
