package telemetry

import "time"

type Labels map[string]string

type MetricSink interface {
	Inc(name string, value int64, labels Labels)
	Observe(name string, value float64, labels Labels)
	Timing(name string, start time.Time, labels Labels)
	LogEvent(name string, labels Labels)
}

type NoopMetrics struct{}

func (NoopMetrics) Inc(name string, value int64, labels Labels)        {}
func (NoopMetrics) Observe(name string, value float64, labels Labels)  {}
func (NoopMetrics) Timing(name string, start time.Time, labels Labels) {}
func (NoopMetrics) LogEvent(name string, labels Labels)                {}

func NewNoopMetrics() MetricSink {
	return NoopMetrics{}
}
