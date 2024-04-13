package httpcommunicator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

// RequestOpts contains all information needed to make the request
type RequestOpts struct {
	// Only accept POST, GET, HEAD, POSTFORM
	Method      string
	URL         string
	ContentType string
	Body        io.Reader
	UrlValues   url.Values
}

// Response from Request.
// Call Close() on Body is next worker responsability
type RequestResponse struct {
	Status     string
	StatusCode int
	Body       io.ReadCloser
	Header     http.Header
}

// HTTPCommunicator client performs HTTP requests based on RequestOpts returned by adaptFn
type HTTPCommunicator[I RequestOpts, O *RequestResponse] struct {
	// HTTP client to be used to perform requests
	client *http.Client
	logger *slog.Logger
}

func NewHTTPCommunicator[I RequestOpts, O *RequestResponse](client *http.Client, logger *slog.Logger) *HTTPCommunicator[I, O] {
	hc := new(HTTPCommunicator[I, O])

	hc.client = client
	hc.logger = logger

	return hc
}

func (hc *HTTPCommunicator[I, O]) Run(_ context.Context, i I, ctx map[string]interface{}, _ string) (O, error) {
	req := RequestOpts(i)

	var resp *http.Response
	var err error

	switch req.Method {
	case "POST":
		resp, err = hc.client.Post(req.URL, req.ContentType, req.Body)
	case "GET":
		resp, err = hc.client.Get(req.URL)
	case "HEAD":
		resp, err = hc.client.Head(req.URL)
	case "POSTFORM":
		resp, err = hc.client.PostForm(req.URL, req.UrlValues)
	default:
		return nil, errors.New("unknown request type given to HTTPCommunicator")
	}

	if err != nil {
		return nil, err
	}

	return &RequestResponse{
		Status:     resp.Status,
		StatusCode: resp.StatusCode,
		Body:       resp.Body,
		Header:     resp.Header,
	}, nil
}
