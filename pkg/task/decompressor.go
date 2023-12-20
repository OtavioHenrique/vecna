package task

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/klauspost/compress/snappy"
	"github.com/klauspost/compress/zstd"
)

const (
	GZIP_TYPE    = "gzip"
	ZLIB_TYPE    = "zlib"
	DEFLATE_TYPE = "deflate"
	SNAPPY_TYPE  = "snappy"
	ZSTD_TYPE    = "zstd"
)

func NewReader(compressionType string, reader io.Reader) (io.Reader, error) {
	var compressor io.Reader
	var err error
	switch compressionType {
	case GZIP_TYPE:
		compressor, err = gzip.NewReader(reader)
	case ZLIB_TYPE:
		compressor, err = zlib.NewReader(reader)
	case DEFLATE_TYPE:
		compressor = flate.NewReader(reader)
	case SNAPPY_TYPE:
		compressor = snappy.NewReader(reader)
	case ZSTD_TYPE:
		compressor, err = zstd.NewReader(reader)
	default:
		return nil, errors.New("unknown-compression-type")
	}

	if err != nil {
		return nil, err
	}

	return compressor, nil
}

type AdaptFn func(interface{}, map[string]interface{}) ([]byte, error)

type Decompressor struct {
	compressionType string
	adaptFn         AdaptFn
	logger          *slog.Logger
}

func NewDecompressor(compressionType string, adaptFn AdaptFn, logger *slog.Logger) *Decompressor {
	d := new(Decompressor)

	d.compressionType = compressionType
	d.adaptFn = adaptFn
	d.logger = logger

	return d
}

func (d *Decompressor) Run(_ context.Context, input interface{}, meta map[string]interface{}, _ string) (interface{}, error) {
	b, err := d.adaptFn(input, meta)

	if err != nil {
		return nil, err
	}

	r := bytes.NewReader(b)
	reader, err := NewReader(d.compressionType, r)
	if err != nil {
		return nil, err
	}

	decompressed, err := io.ReadAll(reader)

	if err != nil {
		return nil, err
	}

	return decompressed, nil
}
