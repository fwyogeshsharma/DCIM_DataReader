package ingest

import "sync/atomic"

func atomicLoadUint32(p *uint32) uint32          { return atomic.LoadUint32(p) }
func atomicAddUint32(p *uint32, d uint32) uint32 { return atomic.AddUint32(p, d) }
