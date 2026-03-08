package httpadapter

import (
	"github.com/noisesoft/Wctlib/pkg/wctlib/adapter"
)

func ExampleAdapter() adapter.TransportAdapter {
	return New("http://localhost:8080")
}
