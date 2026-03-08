package security

type TokenValidator func(token string) (map[string]any, error)

type SignatureVerifier interface {
	Verify(payload []byte, signature []byte) error
}

func NoopVerify(_ []byte, _ []byte) error {
	return nil
}
