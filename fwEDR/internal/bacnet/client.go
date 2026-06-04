package bacnet

import (
	"net"
	"time"
)

// readOpts tunes one device read pass.
type readOpts struct {
	timeout     time.Duration
	retries     int
	objsPerRead int
}

// readObjects reads present-value for every object via chunked
// ReadPropertyMultiple over the connected UDP socket. Chunking keeps each
// request inside the BACnet APDU limit. Returns one PropResult per object that
// answered; objects that time out or error are simply omitted.
func readObjects(conn *net.UDPConn, objs []objMeta, opt readOpts, nextInvoke func() int) []PropResult {
	per := opt.objsPerRead
	if per <= 0 {
		per = 12
	}
	var results []PropResult
	for start := 0; start < len(objs); start += per {
		end := start + per
		if end > len(objs) {
			end = len(objs)
		}
		specs := make([]ObjSpec, 0, end-start)
		for _, o := range objs[start:end] {
			specs = append(specs, ObjSpec{ObjType: o.objType, Instance: o.inst, Props: []int{PropPresentValue}})
		}
		invoke := nextInvoke()
		req := buildReadPropertyMultiple(invoke, specs)
		if part := sendAndDecode(conn, req, invoke, opt); part != nil {
			results = append(results, part...)
		}
	}
	return results
}

// sendAndDecode writes one request and waits for the matching ComplexAck,
// retrying on timeout. Stray datagrams (COV notifications, I-Am, mismatched
// invoke ids) are skipped until the deadline.
func sendAndDecode(conn *net.UDPConn, req []byte, invoke int, opt readOpts) []PropResult {
	buf := make([]byte, 1500)
	for attempt := 0; attempt <= opt.retries; attempt++ {
		if _, err := conn.Write(req); err != nil {
			return nil
		}
		deadline := time.Now().Add(opt.timeout)
		_ = conn.SetReadDeadline(deadline)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				break // timeout / error → retry the whole request
			}
			pdu := parseFrame(buf[:n])
			if pdu == nil {
				continue
			}
			if pdu.kind == pduComplexAck && pdu.invokeID == (invoke&0xFF) &&
				pdu.service == svcReadPropertyMultiple {
				return decodeReadPropertyMultipleAck(pdu.data)
			}
			// not ours (e.g. COV notification) — keep reading until the deadline
			if time.Now().After(deadline) {
				break
			}
		}
	}
	return nil
}

// parseFrame unwraps BVLL→NPDU→APDU and returns the APDU, or nil on any error.
func parseFrame(data []byte) *apdu {
	npdu, err := parseBVLL(data)
	if err != nil {
		return nil
	}
	body, err := parseNPDU(npdu)
	if err != nil {
		return nil
	}
	pdu, err := parseAPDU(body)
	if err != nil {
		return nil
	}
	return pdu
}
