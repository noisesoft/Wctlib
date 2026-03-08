package adapter

import "fmt"

type ErrorCode string

const (
	ErrCodeProbe       ErrorCode = "probe"
	ErrCodeConnect     ErrorCode = "connect"
	ErrCodeTransport   ErrorCode = "transport"
	ErrCodeUnsupported ErrorCode = "unsupported"
)

type Error struct {
	Adapter string
	Code    ErrorCode
	Reason  string
	Err     error
}

func (e Error) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("%s:%s:%s", e.Adapter, e.Code, e.Reason)
	}
	return fmt.Sprintf("%s:%s:%s: %v", e.Adapter, e.Code, e.Reason, e.Err)
}

func (e Error) Unwrap() error {
	return e.Err
}
