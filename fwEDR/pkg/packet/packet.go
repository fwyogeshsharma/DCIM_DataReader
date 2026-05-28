// Package packet handles ed25519 signing and verification of TelemetryPackets.
package packet

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Signer signs packets with an ed25519 private key.
type Signer struct {
	priv     ed25519.PrivateKey
	sourceID string
	nonce    uint64
}

// NewSigner generates a fresh ed25519 key pair. In production, load the key
// from Vault instead of generating at runtime.
func NewSigner(sourceID string) (*Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("packet: generate key: %w", err)
	}
	// Seed nonce from current time so restarts always produce nonces higher
	// than any nonce stored in the DCS Redis dedup layer (24-hour TTL).
	return &Signer{priv: priv, sourceID: sourceID, nonce: uint64(time.Now().UnixNano())}, nil
}

// NewSignerFromKey creates a Signer from an existing private key bytes.
func NewSignerFromKey(sourceID string, privKey []byte) (*Signer, error) {
	if len(privKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("packet: invalid key size %d", len(privKey))
	}
	return &Signer{priv: ed25519.PrivateKey(privKey), sourceID: sourceID}, nil
}

// PublicKey returns the public key bytes for registration with DCS.
func (s *Signer) PublicKey() []byte {
	return s.priv.Public().(ed25519.PublicKey)
}

// NextNonce atomically increments and returns the monotonic nonce.
// Safe for concurrent use by multiple poller goroutines sharing one Signer.
func (s *Signer) NextNonce() uint64 {
	return atomic.AddUint64(&s.nonce, 1)
}

// NewID generates a UUIDv7-style ID (time-ordered UUID).
func NewID() string {
	// UUIDv7: first 48 bits = unix ms, next 4 = version, rest = random
	ms := uint64(time.Now().UnixMilli())
	b := make([]byte, 16)
	binary.BigEndian.PutUint64(b[:8], ms<<16)
	rand.Read(b[6:])            //nolint:errcheck
	b[6] = (b[6] & 0x0f) | 0x70 // version 7
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	u, _ := uuid.FromBytes(b)
	return u.String()
}

// Sign produces an ed25519 signature over the canonical byte representation
// of a packet's immutable fields.
func (s *Signer) Sign(msg []byte) []byte {
	return ed25519.Sign(s.priv, msg)
}

// Verifier verifies packet signatures.
type Verifier struct {
	pub ed25519.PublicKey
}

// NewVerifier creates a Verifier from a public key.
func NewVerifier(pubKey []byte) (*Verifier, error) {
	if len(pubKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("packet: invalid public key size %d", len(pubKey))
	}
	return &Verifier{pub: ed25519.PublicKey(pubKey)}, nil
}

// Verify returns true if the signature is valid for msg.
func (v *Verifier) Verify(msg, sig []byte) bool {
	return ed25519.Verify(v.pub, msg, sig)
}

// CanonicalBytes serializes the fields that are included in the signature.
// Must be identical on both signer (IDR/EDR) and verifier (DCS).
func CanonicalBytes(id, sourceID string, timestampNs int64, name, tag string, value float64, nonce uint64) []byte {
	b := make([]byte, 0, 128)
	b = append(b, []byte(id)...)
	b = append(b, 0)
	b = append(b, []byte(sourceID)...)
	b = append(b, 0)
	ts := make([]byte, 8)
	binary.BigEndian.PutUint64(ts, uint64(timestampNs))
	b = append(b, ts...)
	b = append(b, []byte(name)...)
	b = append(b, 0)
	b = append(b, []byte(tag)...)
	b = append(b, 0)
	vb := make([]byte, 8)
	binary.BigEndian.PutUint64(vb, uint64(value*1e9))
	b = append(b, vb...)
	nb := make([]byte, 8)
	binary.BigEndian.PutUint64(nb, nonce)
	b = append(b, nb...)
	return b
}
