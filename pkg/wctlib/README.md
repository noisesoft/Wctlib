```
Wctlib

Wctlib is an embeddable streaming library for files and media

Core and transport adapters with core plus adapters architecture
Public facade in pkg/wctlib/wctlib.go
Pluggable checkpoint store with durable first policy
DedupStore and FallbackStore for progress recovery and fast read path
MaxBytesPerSec limit in StreamProfile
Built in http, grpc, and webrtc adapters
Unified error contract in adapter.Error
Adaptive FlowController and bounded queues
Adapter switching through adapter.Registry with probing and quarantine
Runtime metrics through telemetry.MetricSink
Receive mode writes to StreamRequest.Sink and must be set

Quick start
```
```
package main

import (
	"context"
	"os"

	wctlib "github.com/noisesoft/Wctlib/pkg/wctlib"
	httpadapter "github.com/noisesoft/Wctlib/pkg/wctlib/adapter/http"
)

func main() {
	ctx := context.Background()
	lib, _ := wctlib.New(wctlib.WithWorkers(8))
	lib.RegisterAdapterTransport(httpadapter.New("http://localhost:8080/wctlib/inbound"))

	input, _ := os.Open("/tmp/file.bin")
	defer input.Close()

	session, _ := lib.StartStream(ctx, wctlib.StreamRequest{
		StreamID:  "upload-1",
		Kind:      wctlib.StreamKindBinary,
		Direction: wctlib.DirectionSend,
		Profile:   wctlib.StreamProfile{ChunkSize: 64 * 1024, WindowSize: 128},
		Metadata:  map[string]string{"http.path": "/wctlib/inbound"},
		Payload:   input,
	})
	_ = session.Wait()
}
```

