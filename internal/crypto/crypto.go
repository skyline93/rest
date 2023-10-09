package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/poly1305"
)

// Key holds encryption and message authentication keys for a repository. It is stored
// encrypted and authenticated as a JSON data structure in the Data field of the Key
// structure.
type Key struct {
	MACKey        `json:"mac"`
	EncryptionKey `json:"encrypt"`
}

// EncryptionKey is key used for encryption
type EncryptionKey [32]byte

// MACKey is used to sign (authenticate) data.
type MACKey struct {
	K [16]byte // for AES-128
	R [16]byte // for Poly1305

	masked bool // remember if the MAC key has already been masked
}

const (
	aesKeySize  = 32                        // for AES-256
	macKeySizeK = 16                        // for AES-128
	macKeySizeR = 16                        // for Poly1305
	macKeySize  = macKeySizeK + macKeySizeR // for Poly1305-AES128
	ivSize      = aes.BlockSize

	macSize = poly1305.TagSize

	// Extension is the number of bytes a plaintext is enlarged by encrypting it.
	Extension = ivSize + macSize
)

// construct mac key from slice (k||r), with masking
func macKeyFromSlice(mk *MACKey, data []byte) {
	copy(mk.K[:], data[:16])
	copy(mk.R[:], data[16:32])
	maskKey(mk)
}

// mask poly1305 key
func maskKey(k *MACKey) {
	if k == nil || k.masked {
		return
	}

	for i := 0; i < poly1305.TagSize; i++ {
		k.R[i] = k.R[i] & poly1305KeyMask[i]
	}

	k.masked = true
}

// mask for key, (cf. http://cr.yp.to/mac/poly1305-20050329.pdf)
var poly1305KeyMask = [16]byte{
	0xff,
	0xff,
	0xff,
	0x0f, // 3: top four bits zero
	0xfc, // 4: bottom two bits zero
	0xff,
	0xff,
	0x0f, // 7: top four bits zero
	0xfc, // 8: bottom two bits zero
	0xff,
	0xff,
	0x0f, // 11: top four bits zero
	0xfc, // 12: bottom two bits zero
	0xff,
	0xff,
	0x0f, // 15: top four bits zero
}

// NewRandomKey returns new encryption and message authentication keys.
func NewRandomKey() *Key {
	k := &Key{}

	n, err := rand.Read(k.EncryptionKey[:])
	if n != aesKeySize || err != nil {
		panic("unable to read enough random bytes for encryption key")
	}

	n, err = rand.Read(k.MACKey.K[:])
	if n != macKeySizeK || err != nil {
		panic("unable to read enough random bytes for MAC encryption key")
	}

	n, err = rand.Read(k.MACKey.R[:])
	if n != macKeySizeR || err != nil {
		panic("unable to read enough random bytes for MAC key")
	}

	maskKey(&k.MACKey)
	return k
}

// NewRandomNonce returns a new random nonce. It panics on error so that the
// program is safely terminated.
func NewRandomNonce() []byte {
	iv := make([]byte, ivSize)
	n, err := rand.Read(iv)
	if n != ivSize || err != nil {
		panic("unable to read enough random bytes for iv")
	}
	return iv
}

// Seal encrypts and authenticates plaintext, authenticates the
// additional data and appends the result to dst, returning the updated
// slice. The nonce must be NonceSize() bytes long and unique for all
// time, for a given key.
//
// The plaintext and dst may alias exactly or not at all. To reuse
// plaintext's storage for the encrypted output, use plaintext[:0] as dst.
func (k *Key) Seal(dst, nonce, plaintext, additionalData []byte) []byte {
	if !k.Valid() {
		panic("key is invalid")
	}

	if len(additionalData) > 0 {
		panic("additional data is not supported")
	}

	if len(nonce) != ivSize {
		panic("incorrect nonce length")
	}

	if !validNonce(nonce) {
		panic("nonce is invalid")
	}

	ret, out := sliceForAppend(dst, len(plaintext)+k.Overhead())

	c, err := aes.NewCipher(k.EncryptionKey[:])
	if err != nil {
		panic(fmt.Sprintf("unable to create cipher: %v", err))
	}
	e := cipher.NewCTR(c, nonce)
	e.XORKeyStream(out, plaintext)

	mac := poly1305MAC(out[:len(plaintext)], nonce, &k.MACKey)
	copy(out[len(plaintext):], mac)

	return ret
}

// Valid tests if the key is valid.
func (k *Key) Valid() bool {
	return k.EncryptionKey.Valid() && k.MACKey.Valid()
}

// Valid tests whether the key k is valid (i.e. not zero).
func (k *EncryptionKey) Valid() bool {
	for i := 0; i < len(k); i++ {
		if k[i] != 0 {
			return true
		}
	}

	return false
}

// validNonce checks that nonce is not all zero.
func validNonce(nonce []byte) bool {
	var sum byte
	for _, b := range nonce {
		sum |= b
	}
	return sum > 0
}

// sliceForAppend takes a slice and a requested number of bytes. It returns a
// slice with the contents of the given slice followed by that many bytes and a
// second slice that aliases into it and contains only the extra bytes. If the
// original slice has sufficient capacity then no allocation is performed.
//
// taken from the stdlib, crypto/aes/aes_gcm.go
func sliceForAppend(in []byte, n int) (head, tail []byte) {
	if total := len(in) + n; cap(in) >= total {
		head = in[:total]
	} else {
		head = make([]byte, total)
		copy(head, in)
	}
	tail = head[len(in):]
	return
}

// Overhead returns the maximum difference between the lengths of a
// plaintext and its ciphertext.
func (k *Key) Overhead() int {
	return macSize
}

func poly1305MAC(msg []byte, nonce []byte, key *MACKey) []byte {
	k := poly1305PrepareKey(nonce, key)

	var out [16]byte
	poly1305.Sum(&out, msg, &k)

	return out[:]
}

// prepare key for low-level poly1305.Sum(): r||n
func poly1305PrepareKey(nonce []byte, key *MACKey) [32]byte {
	var k [32]byte

	maskKey(key)

	cipher, err := aes.NewCipher(key.K[:])
	if err != nil {
		panic(err)
	}
	cipher.Encrypt(k[16:], nonce[:])

	copy(k[:16], key.R[:])

	return k
}

// Valid tests whether the key k is valid (i.e. not zero).
func (m *MACKey) Valid() bool {
	nonzeroK := false
	for i := 0; i < len(m.K); i++ {
		if m.K[i] != 0 {
			nonzeroK = true
		}
	}

	if !nonzeroK {
		return false
	}

	for i := 0; i < len(m.R); i++ {
		if m.R[i] != 0 {
			return true
		}
	}

	return false
}
