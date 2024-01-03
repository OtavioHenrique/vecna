package requests

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

type HTTPCommAdaptFn func(interface{}, map[string]interface{}) (RequestOpts, error)

// RequestOpts contains all information needed to make the request
type RequestOpts struct {
	Method      string
	URL         string
	ContentType string
	Body        io.Reader
	UrlValues   url.Values
}

// HTTPCommunicator client performs HTTP requests based on RequestOpts returned by adaptFn
type HTTPCommunicator struct {
	// HTTP client to be used to perform requests
	client  *http.Client
	adaptFn HTTPCommAdaptFn
	logger  *slog.Logger
}

func NewHTTPCommunicator(client *http.Client, adaptFn HTTPCommAdaptFn, logger *slog.Logger) *HTTPCommunicator {
	hc := new(HTTPCommunicator)

	hc.client = client
	hc.adaptFn = adaptFn
	hc.logger = logger

	return hc
}

func (hc *HTTPCommunicator) Run(_ context.Context, i interface{}, ctx map[string]interface{}, _ string) (interface{}, error) {
	req, err := hc.adaptFn(i, ctx)

	if err != nil {
		return nil, err
	}

	var resp *http.Response

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

	return resp, nil
}
