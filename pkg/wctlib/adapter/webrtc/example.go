package webrtcadapter

import "github.com/noisesoft/Wctlib/pkg/wctlib/adapter"

func ExampleAdapter() adapter.TransportAdapter {
	return New("room-live")
}
