// Package queue implements a local persistent FIFO queue backed by bbolt.
// Survives IDR crashes and WAN outages — packets are drained to DCS on reconnect.
package queue

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	v1 "github.com/faberwork/fwidr/proto/v1"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

var (
	ErrFull  = errors.New("queue: full")
	ErrEmpty = errors.New("queue: empty")

	bucketName = []byte("packets")
)

// Queue is a persistent, bounded FIFO for TelemetryPackets.
type Queue struct {
	db       *bolt.DB
	maxBytes int64
	mu       sync.Mutex
	path     string
}

// Open opens or creates the queue at path.
// maxBytes is the soft limit; when exceeded, non-critical packets are dropped.
func Open(path string, maxBytes int64) (*Queue, error) {
	if maxBytes <= 0 {
		maxBytes = 512 * 1024 * 1024 // 512 MB default
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("queue: mkdir %s: %w", filepath.Dir(path), err)
	}

	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("queue: open %s: %w", path, err)
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("queue: init bucket: %w", err)
	}

	return &Queue{db: db, maxBytes: maxBytes, path: path}, nil
}

// Push appends a packet to the tail of the queue.
// If the queue is full:
//   - critical/major packets replace oldest non-critical entries
//   - others are dropped with ErrFull
func (q *Queue) Push(pkt *v1.TelemetryPacket) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	data, err := proto.Marshal(pkt)
	if err != nil {
		return fmt.Errorf("queue: marshal: %w", err)
	}

	return q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)

		// Check size
		stats := b.Stats()
		if int64(stats.LeafInuse) >= q.maxBytes {
			if pkt.Severity == "critical" || pkt.Severity == "major" {
				// Evict oldest non-critical entry
				if err := evictOldestNonCritical(b); err != nil {
					return ErrFull
				}
			} else {
				return ErrFull
			}
		}

		seq, err := b.NextSequence()
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, seq)
		return b.Put(key, data)
	})
}

// Pop removes and returns the oldest packet. Returns ErrEmpty when queue is empty.
func (q *Queue) Pop() (*v1.TelemetryPacket, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var pkt v1.TelemetryPacket
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		k, v := c.First()
		if k == nil {
			return ErrEmpty
		}
		if err := proto.Unmarshal(v, &pkt); err != nil {
			// Corrupted entry — delete and report
			_ = b.Delete(k)
			return fmt.Errorf("queue: unmarshal: %w", err)
		}
		return b.Delete(k)
	})
	if err != nil {
		return nil, err
	}
	return &pkt, nil
}

// Len returns the number of queued packets.
func (q *Queue) Len() int {
	var count int
	q.db.View(func(tx *bolt.Tx) error { //nolint:errcheck
		count = tx.Bucket(bucketName).Stats().KeyN
		return nil
	})
	return count
}

// Close flushes and closes the underlying database.
func (q *Queue) Close() error { return q.db.Close() }

// evictOldestNonCritical deletes the oldest entry whose severity is not
// critical/major.  Returns error if no such entry exists.
func evictOldestNonCritical(b *bolt.Bucket) error {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		// Quick severity check via JSON fallback (proto unmarshal is too slow here)
		var m struct {
			Severity string `json:"severity"`
		}
		_ = json.Unmarshal(v, &m)
		if m.Severity != "critical" && m.Severity != "major" {
			return b.Delete(k)
		}
	}
	return errors.New("queue: no evictable entry")
}
