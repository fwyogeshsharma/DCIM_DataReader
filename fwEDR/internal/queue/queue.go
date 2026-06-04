// Package queue implements a local persistent FIFO queue backed by bbolt.
// Survives EDR crashes and WAN outages — packets are drained to DCS on reconnect.
//
// Batch ops (PushBatch/PopBatch) amortize bbolt's per-transaction fsync cost
// across N packets, raising throughput by 1-2 orders of magnitude vs single-
// packet Push/Pop.
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

	v1 "github.com/faberwork/fwedr/proto/v1"
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

	// usedBytes tracks in-use payload bytes incrementally so the hot push/pop
	// paths never call bbolt's O(n) Bucket.Stats(). Maintained under mu: pushes
	// add, pops/evictions subtract. Seeded once at Open from a single Stats().
	usedBytes int64
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

	q := &Queue{db: db, maxBytes: maxBytes, path: path}
	// Seed usedBytes once from the on-disk bucket so the cap is honored across
	// restarts (a crash can leave an undrained backlog). One Stats() at startup
	// is fine; the hot paths then maintain the counter incrementally.
	_ = db.View(func(tx *bolt.Tx) error {
		q.usedBytes = int64(tx.Bucket(bucketName).Stats().LeafInuse)
		return nil
	})
	return q, nil
}

// Push appends a packet to the tail. Single-packet API kept for compatibility;
// for high-volume paths use PushBatch.
func (q *Queue) Push(pkt *v1.TelemetryPacket) error {
	return q.PushBatch([]*v1.TelemetryPacket{pkt})
}

// PushBatch appends N packets in a single bbolt transaction (one fsync).
// On overflow, criticals evict oldest non-criticals; non-criticals return ErrFull.
func (q *Queue) PushBatch(pkts []*v1.TelemetryPacket) error {
	if len(pkts) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	// Marshal outside the txn so we don't hold the bbolt write lock during
	// proto serialization.
	type kv struct {
		data     []byte
		severity string
	}
	entries := make([]kv, 0, len(pkts))
	for _, pkt := range pkts {
		data, err := proto.Marshal(pkt)
		if err != nil {
			return fmt.Errorf("queue: marshal: %w", err)
		}
		entries = append(entries, kv{data: data, severity: pkt.Severity})
	}

	// Work on a local copy of the byte counter; commit it to q.usedBytes only
	// after the txn succeeds. bbolt rolls the whole txn back on any returned
	// error (including ErrFull), so a partial batch never persists — the counter
	// must mirror that all-or-nothing semantics.
	used := q.usedBytes
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, e := range entries {
			if used >= q.maxBytes {
				if e.severity == "critical" || e.severity == "major" {
					freed, everr := evictOldestNonCritical(b)
					if everr != nil {
						return ErrFull
					}
					used -= freed
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
			if err := b.Put(key, e.data); err != nil {
				return err
			}
			used += int64(len(e.data))
		}
		return nil
	})
	if err != nil {
		return err
	}
	q.usedBytes = used
	return nil
}

// Pop removes and returns the oldest packet. Single-packet API kept for
// compatibility; for high-volume paths use PopBatch.
func (q *Queue) Pop() (*v1.TelemetryPacket, error) {
	pkts, err := q.PopBatch(1)
	if err != nil {
		return nil, err
	}
	if len(pkts) == 0 {
		return nil, ErrEmpty
	}
	return pkts[0], nil
}

// PopBatch removes up to n packets from the head in a single transaction.
// Returns ErrEmpty only if the queue is empty; partial batches return what
// was available with no error.
func (q *Queue) PopBatch(n int) ([]*v1.TelemetryPacket, error) {
	if n <= 0 {
		return nil, nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()

	var rawValues [][]byte
	var freed int64
	err := q.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		c := b.Cursor()
		// Collect keys+values in one forward pass (no per-delete cursor restart,
		// which was O(n²) and throttled the drain), then delete by key. bbolt
		// returns slices into the mmap that go invalid once the txn ends, so copy.
		keys := make([][]byte, 0, n)
		for k, v := c.First(); k != nil && len(rawValues) < n; k, v = c.Next() {
			vc := make([]byte, len(v))
			copy(vc, v)
			rawValues = append(rawValues, vc)
			kc := make([]byte, len(k))
			copy(kc, k)
			keys = append(keys, kc)
			freed += int64(len(v))
		}
		if len(rawValues) == 0 {
			return ErrEmpty
		}
		for _, k := range keys {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if q.usedBytes -= freed; q.usedBytes < 0 {
		q.usedBytes = 0
	}

	out := make([]*v1.TelemetryPacket, 0, len(rawValues))
	for _, v := range rawValues {
		pkt := &v1.TelemetryPacket{}
		if err := proto.Unmarshal(v, pkt); err != nil {
			// Drop corrupted entry; already deleted from queue.
			continue
		}
		out = append(out, pkt)
	}
	return out, nil
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
// critical/major and returns the number of payload bytes freed. Returns an
// error if no such entry exists.
func evictOldestNonCritical(b *bolt.Bucket) (int64, error) {
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		var m struct {
			Severity string `json:"severity"`
		}
		_ = json.Unmarshal(v, &m)
		if m.Severity != "critical" && m.Severity != "major" {
			n := int64(len(v))
			if err := b.Delete(k); err != nil {
				return 0, err
			}
			return n, nil
		}
	}
	return 0, errors.New("queue: no evictable entry")
}
