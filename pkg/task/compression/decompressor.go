package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"

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

// Decompressor is a generic task capable of decompress []byte compressed with ZSTD or GZIP
type Decompressor[T, K []byte] struct {
	// Compression type which it will focus on decompressing. Must be either zstd or gzip
	compressionType string
	logger          *slog.Logger
}

func NewDecompressor[I, O []byte](compressionType string, logger *slog.Logger) *Decompressor[I, O] {
	d := new(Decompressor[I, O])

	d.compressionType = compressionType
	d.logger = logger

	return d
}

// The return data from Run will be []byte decompressed
func (d *Decompressor[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	r := bytes.NewReader(input)
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
