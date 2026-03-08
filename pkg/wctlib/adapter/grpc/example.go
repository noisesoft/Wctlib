package grpcadapter

import "github.com/noisesoft/Wctlib/pkg/wctlib/adapter"

func ExampleAdapter() adapter.TransportAdapter {
	return New("grpc://localhost:50051")
}
