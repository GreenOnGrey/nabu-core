// Package crypto encrypts OAuth tokens at rest with AES-GCM.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// Box encrypts and decrypts small secrets.
type Box struct{ aead cipher.AEAD }

// NewBox builds a Box from a 32-byte key.
func NewBox(key []byte) (*Box, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &Box{aead: aead}, nil
}

// Seal encrypts plaintext; the nonce is prepended to the ciphertext.
func (b *Box) Seal(plaintext string) ([]byte, error) {
	nonce := make([]byte, b.aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return b.aead.Seal(nonce, nonce, []byte(plaintext), nil), nil
}

// Open decrypts data produced by Seal.
func (b *Box) Open(data []byte) (string, error) {
	n := b.aead.NonceSize()
	if len(data) < n {
		return "", errors.New("ciphertext too short")
	}
	pt, err := b.aead.Open(nil, data[:n], data[n:], nil)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
