package crypto

// CiphertextLength returns the encrypted length of a blob with plaintextSize
// bytes.
func CiphertextLength(plaintextSize int) int {
	return plaintextSize + Extension
}

// NewBlobBuffer returns a buffer that is large enough to hold a blob of size
// plaintext bytes, including the crypto overhead.
func NewBlobBuffer(size int) []byte {
	return make([]byte, size, size+Extension)
}
