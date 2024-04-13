package compression

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

var (
	ErrUnknownCompressionType = errors.New("unknown-compression-type")
)

// Compressor is a generic task capable of compress a []byte into zstd or gzip.
type Compressor[I, O []byte] struct {
	// CompressionType is either "gzip" or "zstd"
	compressionType string
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
		return nil, ErrUnknownCompressionType
	}

	if err != nil {
		return nil, err
	}

	return compressor, nil
}

// Creates a new Compressor task.
func NewCompressor[T, K []byte](compressionType string, logger *slog.Logger) *Compressor[T, K] {
	d := new(Compressor[T, K])

	d.compressionType = compressionType
	d.logger = logger

	return d
}

// Run receives output from previous worker, calls adaptFn and compress it into selected format.
func (d *Compressor[I, O]) Run(_ context.Context, input I, meta map[string]interface{}, _ string) (O, error) {
	var buf bytes.Buffer
	zw, err := NewWriter(d.compressionType, &buf)
	if err != nil {
		return nil, err
	}

	_, err = zw.Write(input)
	if err != nil {
		return nil, err
	}

	zw.Close()

	result := buf.Bytes()
	os.WriteFile("/tmp/dat5", result, 0644)

	return result, nil
}
