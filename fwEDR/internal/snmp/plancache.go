package snmp

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// PlanStore caches per-device PollPlans so discovery walks are not repeated every
// poll (or every restart). Two implementations:
//   - MemStore: in-process only (L1). Rebuilt on restart via discovery.
//   - RedisStore: L1 memory + L2 Redis for cross-restart persistence — skip the
//     startup re-walk when a fresh plan exists. Redis is best-effort: any Redis
//     error degrades to memory-only, never fails collection.
//
// deviceID here is the caller's stable device key (e.g. target SourceID / mgmt_ip).
type PlanStore interface {
	Get(ctx context.Context, deviceID string) (*PollPlan, bool)
	Put(ctx context.Context, deviceID string, plan *PollPlan) error
	Close() error
}

// ─── L1: in-memory ────────────────────────────────────────────────────────────

type MemStore struct {
	mu sync.RWMutex
	m  map[string]*PollPlan
}

func NewMemStore() *MemStore { return &MemStore{m: make(map[string]*PollPlan)} }

func (s *MemStore) Get(_ context.Context, deviceID string) (*PollPlan, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.m[deviceID]
	return p, ok
}

func (s *MemStore) Put(_ context.Context, deviceID string, plan *PollPlan) error {
	s.mu.Lock()
	s.m[deviceID] = plan
	s.mu.Unlock()
	return nil
}

func (s *MemStore) Close() error { return nil }

// ─── L2: memory + Redis ───────────────────────────────────────────────────────

type RedisStore struct {
	l1  *MemStore
	rdb *redis.Client
	ttl time.Duration
	log *zap.Logger
}

// NewRedisStore builds a memory+Redis plan store. ttl bounds how long a persisted
// plan is trusted before a re-walk (set ~ rediscovery interval + margin). A nil/
// unreachable Redis still yields a working memory-only store.
func NewRedisStore(addr, password string, db int, ttl time.Duration, log *zap.Logger) *RedisStore {
	rdb := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	return &RedisStore{l1: NewMemStore(), rdb: rdb, ttl: ttl, log: log}
}

func redisKey(deviceID string) string { return "edr:oidset:" + deviceID }

func (s *RedisStore) Get(ctx context.Context, deviceID string) (*PollPlan, bool) {
	if p, ok := s.l1.Get(ctx, deviceID); ok {
		return p, true // L1 hit — no Redis round-trip
	}
	val, err := s.rdb.Get(ctx, redisKey(deviceID)).Bytes()
	if err != nil {
		if err != redis.Nil && s.log != nil {
			s.log.Debug("plan cache: redis get failed — treating as miss",
				zap.String("device", deviceID), zap.Error(err))
		}
		return nil, false
	}
	var p PollPlan
	if err := json.Unmarshal(val, &p); err != nil {
		return nil, false
	}
	_ = s.l1.Put(ctx, deviceID, &p) // populate L1
	return &p, true
}

func (s *RedisStore) Put(ctx context.Context, deviceID string, plan *PollPlan) error {
	_ = s.l1.Put(ctx, deviceID, plan)
	data, err := json.Marshal(plan)
	if err != nil {
		return err // memory still holds it; caller ignores this
	}
	if err := s.rdb.Set(ctx, redisKey(deviceID), data, s.ttl).Err(); err != nil {
		if s.log != nil {
			s.log.Debug("plan cache: redis set failed — memory-only for now",
				zap.String("device", deviceID), zap.Error(err))
		}
		// Not fatal: L1 has the plan; persistence just lags until Redis recovers.
	}
	return nil
}

func (s *RedisStore) Close() error { return s.rdb.Close() }

// PlanFresh reports whether a plan is younger than maxAge (0 = always fresh). Used
// on startup to decide whether a cached plan can skip the discovery re-walk.
func PlanFresh(p *PollPlan, maxAge time.Duration, nowNs int64) bool {
	if p == nil {
		return false
	}
	if maxAge <= 0 {
		return true
	}
	return time.Duration(nowNs-p.WalkedAt) < maxAge
}
