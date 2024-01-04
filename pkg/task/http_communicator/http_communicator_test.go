package httpcommunicator_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	httpcommunicator "github.com/otaviohenrique/vecna/pkg/task/http_communicator"
)

func TestHTTPCommunicator_Run(t *testing.T) {
	type fields struct {
		svr     *httptest.Server
		client  *http.Client
		adaptFn httpcommunicator.HTTPCommAdaptFn
		logger  *slog.Logger
	}
	type args struct {
		in0 context.Context
		i   interface{}
		ctx map[string]interface{}
		in3 string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    httpcommunicator.RequestResponse
		wantErr bool
	}{
		{"It correctly do POST request", func() fields {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					w.WriteHeader(http.StatusOK)
					reqBody, _ := io.ReadAll(r.Body)
					fmt.Fprintf(w, string(reqBody))
				}
			}))

			return fields{
				svr:    server,
				logger: slog.New(slog.NewTextHandler(os.Stdout, nil)),
				client: http.DefaultClient,
				adaptFn: func(i interface{}, _ map[string]interface{}) (httpcommunicator.RequestOpts, error) {
					return httpcommunicator.RequestOpts{
						Method: "POST",
						URL:    server.URL,
						Body:   bytes.NewReader([]byte("OK")),
					}, nil
				},
			}
		}(),
			args{
				in0: context.TODO(),
				i:   nil,
				ctx: nil,
				in3: "Test Task Worker",
			},
			httpcommunicator.RequestResponse{
				Status:     "200 OK",
				StatusCode: 200,
				Header:     http.Header{},
				Body:       io.NopCloser(bytes.NewReader([]byte("OK"))),
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := httpcommunicator.NewHTTPCommunicator(
				tt.fields.client,
				tt.fields.adaptFn,
				tt.fields.logger,
			)
			got, err := hc.Run(tt.args.in0, tt.args.i, tt.args.ctx, tt.args.in3)
			if (err != nil) != tt.wantErr {
				t.Errorf("HTTPCommunicator.Run() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				resp := got.(*httpcommunicator.RequestResponse)

				if resp.StatusCode != tt.want.StatusCode {
					t.Errorf("HTTPCommunicator.Run() statusCode = %v, want %v", resp.StatusCode, tt.want.StatusCode)
				}

				if resp.Status != tt.want.Status {
					t.Errorf("HTTPCommunicator.Run() status = %v, want %v", resp.StatusCode, tt.want.StatusCode)
				}

				body, _ := io.ReadAll(resp.Body)
				expectBody, _ := io.ReadAll(tt.want.Body)

				if string(body) != string(expectBody) {
					t.Errorf("HTTPCommunicator.Run() body = %v, want %v", resp.StatusCode, tt.want.StatusCode)
				}
			}
		})
	}
}
