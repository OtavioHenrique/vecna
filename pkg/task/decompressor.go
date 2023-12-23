package task

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

type DecompressAdaptFn func(interface{}, map[string]interface{}) ([]byte, error)

// Decompressor is a generic task capable of decompress []byte compressed with ZSTD or GZIP
type Decompressor struct {
	// Compression type which it will focus on decompressing. Must be either zstd or gzip
	compressionType string
	// adaptFn which will receive interface{} from previous worker and return []byte to be decompressed
	adaptFn DecompressAdaptFn
	logger  *slog.Logger
}

func NewDecompressor(compressionType string, adaptFn DecompressAdaptFn, logger *slog.Logger) *Decompressor {
	d := new(Decompressor)

	d.compressionType = compressionType
	d.adaptFn = adaptFn
	d.logger = logger

	return d
}

// The return data from Run will be []byte decompressed
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
