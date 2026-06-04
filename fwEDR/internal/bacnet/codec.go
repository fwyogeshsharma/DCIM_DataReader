// Package bacnet implements the minimal BACnet/IP client subset EDR needs to
// poll Verdigris EV2 energy monitors: ReadPropertyMultiple (and ReadProperty),
// Who-Is/I-Am discovery, and SubscribeCOV + COV notification handling.
//
// Pure encode/decode — no third-party BACnet dependency. The wire format mirrors
// the simulator's core/bacnet_object_model.py and was validated end-to-end with
// testscripts/edr_bacnet_probe.py.
package bacnet

import (
	"encoding/binary"
	"errors"
	"math"
)

// ─── object types / property ids / services ──────────────────────────────────

const (
	ObjAnalogInput = 0
	ObjBinaryInput = 3
	ObjDevice      = 8

	PropObjectName   = 77
	PropObjectList   = 76
	PropPresentValue = 85
	PropStatusFlags  = 111
	PropUnits        = 117

	pduConfirmedRequest   = 0
	pduUnconfirmedRequest = 1
	pduComplexAck         = 3
	pduError              = 5

	svcIAm                  = 0
	svcUnconfirmedCOVNotify = 2
	svcWhoIs                = 8
	svcSubscribeCOV         = 5
	svcReadProperty         = 12
	svcReadPropertyMultiple = 14

	bvllType              = 0x81
	bvlcOriginalUnicast   = 0x0A
	bvlcOriginalBroadcast = 0x0B
	bvlcForwardedNPDU     = 0x04

	maxAPDULength = 1476
)

// ErrShort is returned when a frame is too short to parse.
var ErrShort = errors.New("bacnet: short frame")

// ─── application/context tag encoders ─────────────────────────────────────────

func encAppUint(val uint32) []byte {
	if val == 0 {
		return []byte{0x21, 0x00}
	}
	n := (bitLen(val) + 7) / 8
	if n > 4 {
		n = 4
	}
	out := []byte{byte((2 << 4) | n)}
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, val)
	return append(out, tmp[4-n:]...)
}

func encAppEnum(val uint32) []byte {
	n := (bitLen(val) + 7) / 8
	if n < 1 {
		n = 1
	}
	if n > 4 {
		n = 4
	}
	out := []byte{byte((9 << 4) | n)}
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, val)
	return append(out, tmp[4-n:]...)
}

func encAppOID(objType, instance uint32) []byte {
	val := ((objType & 0x3FF) << 22) | (instance & 0x3FFFFF)
	out := []byte{0xC4}
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, val)
	return append(out, tmp...)
}

func encCtxUint(ctx int, val uint32) []byte {
	var data []byte
	if val == 0 {
		data = []byte{0x00}
	} else {
		n := (bitLen(val) + 7) / 8
		tmp := make([]byte, 4)
		binary.BigEndian.PutUint32(tmp, val)
		data = tmp[4-n:]
	}
	return append([]byte{byte((ctx << 4) | 0x08 | len(data))}, data...)
}

func encCtxOID(ctx, objType, instance int) []byte {
	val := ((uint32(objType) & 0x3FF) << 22) | (uint32(instance) & 0x3FFFFF)
	out := []byte{byte((ctx << 4) | 0x08 | 4)}
	tmp := make([]byte, 4)
	binary.BigEndian.PutUint32(tmp, val)
	return append(out, tmp...)
}

func encCtxOpen(ctx int) []byte  { return []byte{byte((ctx << 4) | 0x0E)} }
func encCtxClose(ctx int) []byte { return []byte{byte((ctx << 4) | 0x0F)} }

func bitLen(v uint32) int {
	n := 0
	for v > 0 {
		n++
		v >>= 1
	}
	return n
}

// ─── BVLL / NPDU framing ──────────────────────────────────────────────────────

func buildNPDU(apdu []byte, expectsReply bool) []byte {
	ctrl := byte(0x00)
	if expectsReply {
		ctrl = 0x04
	}
	return append([]byte{0x01, ctrl}, apdu...)
}

func buildBVLL(npdu []byte, broadcast bool) []byte {
	fn := byte(bvlcOriginalUnicast)
	if broadcast {
		fn = bvlcOriginalBroadcast
	}
	length := 4 + len(npdu)
	return append([]byte{bvllType, fn, byte(length >> 8), byte(length & 0xFF)}, npdu...)
}

func parseBVLL(data []byte) ([]byte, error) {
	if len(data) < 4 || data[0] != bvllType {
		return nil, ErrShort
	}
	length := int(data[2])<<8 | int(data[3])
	if length > len(data) {
		return nil, ErrShort
	}
	if data[1] == bvlcForwardedNPDU {
		if length < 10 {
			return nil, ErrShort
		}
		return data[10:length], nil
	}
	return data[4:length], nil
}

func parseNPDU(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x01 {
		return nil, ErrShort
	}
	ctrl := data[1]
	if ctrl&0x80 != 0 {
		return nil, errors.New("bacnet: network-layer message")
	}
	pos := 2
	if ctrl&0x20 != 0 { // DNET
		if pos+2 >= len(data) {
			return nil, ErrShort
		}
		dlen := int(data[pos+2])
		pos += 3 + dlen + 1
	}
	if ctrl&0x08 != 0 { // SNET
		if pos+2 >= len(data) {
			return nil, ErrShort
		}
		slen := int(data[pos+2])
		pos += 3 + slen
	}
	if pos > len(data) {
		return nil, ErrShort
	}
	return data[pos:], nil
}

// apdu is the parsed APDU header + payload.
type apdu struct {
	kind     int
	service  int
	invokeID int
	data     []byte
}

func parseAPDU(data []byte) (*apdu, error) {
	if len(data) == 0 {
		return nil, ErrShort
	}
	t := int((data[0] >> 4) & 0x0F)
	switch t {
	case pduUnconfirmedRequest:
		if len(data) < 2 {
			return nil, ErrShort
		}
		return &apdu{kind: t, service: int(data[1]), data: data[2:]}, nil
	case pduComplexAck:
		if len(data) < 3 {
			return nil, ErrShort
		}
		return &apdu{kind: t, invokeID: int(data[1]), service: int(data[2]), data: data[3:]}, nil
	case pduError:
		if len(data) < 3 {
			return nil, ErrShort
		}
		return &apdu{kind: t, invokeID: int(data[1]), service: int(data[2]), data: data[3:]}, nil
	}
	return nil, errors.New("bacnet: unhandled apdu type")
}

// ─── request builders ─────────────────────────────────────────────────────────

// ObjSpec names an object and the properties to read from it.
type ObjSpec struct {
	ObjType  int
	Instance int
	Props    []int
}

// buildReadPropertyMultiple builds a confirmed ReadPropertyMultiple request.
func buildReadPropertyMultiple(invokeID int, specs []ObjSpec) []byte {
	apdu := []byte{
		byte(pduConfirmedRequest << 4),
		0x05, // max-segs=0 | max-apdu=5 (1476)
		byte(invokeID & 0xFF),
		svcReadPropertyMultiple,
	}
	for _, s := range specs {
		apdu = append(apdu, encCtxOID(0, s.ObjType, s.Instance)...)
		apdu = append(apdu, encCtxOpen(1)...)
		for _, p := range s.Props {
			apdu = append(apdu, encCtxUint(0, uint32(p))...)
		}
		apdu = append(apdu, encCtxClose(1)...)
	}
	return buildBVLL(buildNPDU(apdu, true), false)
}

// buildReadProperty builds a confirmed ReadProperty request (single property).
func buildReadProperty(invokeID, objType, instance, prop int) []byte {
	apdu := []byte{
		byte(pduConfirmedRequest << 4),
		0x05,
		byte(invokeID & 0xFF),
		svcReadProperty,
	}
	apdu = append(apdu, encCtxOID(0, objType, instance)...)
	apdu = append(apdu, encCtxUint(1, uint32(prop))...)
	return buildBVLL(buildNPDU(apdu, true), false)
}

// buildWhoIs builds a global Who-Is broadcast.
func buildWhoIs() []byte {
	apdu := []byte{byte(pduUnconfirmedRequest << 4), svcWhoIs}
	return buildBVLL(buildNPDU(apdu, false), true)
}

// buildSubscribeCOV builds a confirmed SubscribeCOV request. lifetime=0 with the
// cancel flag would cancel; here we always subscribe (unconfirmed notifications).
func buildSubscribeCOV(invokeID, processID, objType, instance, lifetimeSec int) []byte {
	apdu := []byte{
		byte(pduConfirmedRequest << 4),
		0x05,
		byte(invokeID & 0xFF),
		svcSubscribeCOV,
	}
	apdu = append(apdu, encCtxUint(0, uint32(processID))...) // subscriber process id
	apdu = append(apdu, encCtxOID(1, objType, instance)...)  // monitored object
	apdu = append(apdu, encCtxUint(2, 0)...)                 // issueConfirmed = false
	apdu = append(apdu, encCtxUint(3, uint32(lifetimeSec))...)
	return buildBVLL(buildNPDU(apdu, true), false)
}

// ─── value / response decoders ────────────────────────────────────────────────

// decodeOID splits a 4-byte object identifier into (objType, instance).
func decodeOID(b []byte) (int, int) {
	v := binary.BigEndian.Uint32(b)
	return int(v>>22) & 0x3FF, int(v & 0x3FFFFF)
}

// readTagHdr parses one tag header at data[pos]. Returns tag number, context
// flag, opening, closing, value length, and the new position.
func readTagHdr(data []byte, pos int) (tagNum int, isCtx, isOpen, isClose bool, vlen, newPos int, err error) {
	if pos >= len(data) {
		return 0, false, false, false, 0, pos, ErrShort
	}
	b := data[pos]
	raw := int(b>>4) & 0x0F
	isCtx = b&0x08 != 0
	lenEnc := int(b & 0x07)
	pos++
	if raw == 0x0F {
		if pos >= len(data) {
			return 0, false, false, false, 0, pos, ErrShort
		}
		tagNum = int(data[pos])
		pos++
	} else {
		tagNum = raw
	}
	if isCtx && lenEnc == 6 {
		return tagNum, true, true, false, 0, pos, nil
	}
	if isCtx && lenEnc == 7 {
		return tagNum, true, false, true, 0, pos, nil
	}
	if lenEnc == 5 {
		if pos >= len(data) {
			return 0, false, false, false, 0, pos, ErrShort
		}
		ext := int(data[pos])
		pos++
		switch {
		case ext == 254:
			if pos+2 > len(data) {
				return 0, false, false, false, 0, pos, ErrShort
			}
			vlen = int(data[pos])<<8 | int(data[pos+1])
			pos += 2
		case ext == 255:
			if pos+4 > len(data) {
				return 0, false, false, false, 0, pos, ErrShort
			}
			vlen = int(binary.BigEndian.Uint32(data[pos : pos+4]))
			pos += 4
		default:
			vlen = ext
		}
	} else {
		vlen = lenEnc
	}
	return tagNum, isCtx, false, false, vlen, pos, nil
}

// decodeAppValue decodes one application-tagged primitive into a float64.
// Supports REAL, unsigned, enumerated, boolean, and 4-bit bitstring (status
// flags). Returns the value and the position after the tag+value.
func decodeAppValue(data []byte, pos int) (float64, int, bool) {
	if pos >= len(data) {
		return 0, pos, false
	}
	tag := data[pos]
	appTag := int(tag>>4) & 0x0F
	lenEnc := int(tag & 0x07)
	p := pos + 1
	switch appTag {
	case 1: // boolean — value is the low length bit
		return float64(lenEnc & 1), p, true
	case 2, 9: // unsigned / enumerated
		if p+lenEnc > len(data) {
			return 0, pos, false
		}
		var v uint32
		for i := 0; i < lenEnc; i++ {
			v = v<<8 | uint32(data[p+i])
		}
		return float64(v), p + lenEnc, true
	case 4: // REAL (float32)
		if p+4 > len(data) {
			return 0, pos, false
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(data[p : p+4]))), p + 4, true
	case 8: // bitstring — first byte = unused bits; take next byte's high nibble
		if lenEnc < 1 || p+lenEnc > len(data) {
			return 0, pos, false
		}
		if lenEnc >= 2 {
			return float64(data[p+1] >> 4), p + lenEnc, true
		}
		return 0, p + lenEnc, true
	}
	return 0, pos, false
}

// PropResult is one decoded (object, property) → value.
type PropResult struct {
	ObjType  int
	Instance int
	Prop     int
	Value    float64
	OK       bool // false when the device returned a property error
}

// decodeReadPropertyMultipleAck walks a ReadPropertyMultiple ComplexAck and
// returns the present-value per object. The simulator wraps each property result
// as: context[2] open { context[0] propertyId, context[4] open <value> close } close
// (see bacnet_device._encode_rpm_result) — note this differs from some real
// devices, which omit the per-result context[2] wrapper.
func decodeReadPropertyMultipleAck(data []byte) []PropResult {
	var out []PropResult
	var (
		tn, vlen, np int
		ctx, isOpen  bool
		isClose      bool
		err          error
	)
	pos := 0
	for pos < len(data) {
		// context[0] objectIdentifier
		tn, ctx, _, _, vlen, np, err = readTagHdr(data, pos)
		if err != nil || !ctx || tn != 0 || vlen < 4 || np+4 > len(data) {
			break
		}
		objType, inst := decodeOID(data[np : np+4])
		pos = np + 4
		// context[1] opening listOfResults
		tn, ctx, isOpen, _, _, np, err = readTagHdr(data, pos)
		if err != nil || !ctx || tn != 1 || !isOpen {
			break
		}
		pos = np
		// each result: context[2] open { [0] propId, [4]/[5] open value/error } close
		for pos < len(data) {
			tn, ctx, isOpen, isClose, _, np, err = readTagHdr(data, pos)
			if err != nil {
				return out
			}
			if ctx && tn == 1 && isClose { // end of listOfResults
				pos = np
				break
			}
			if !(ctx && tn == 2 && isOpen) {
				return out
			}
			pos = np
			// context[0] propertyIdentifier
			tn, ctx, _, _, vlen, np, err = readTagHdr(data, pos)
			if err != nil || !ctx || tn != 0 || np+vlen > len(data) {
				return out
			}
			prop := 0
			for i := 0; i < vlen; i++ {
				prop = prop<<8 | int(data[np+i])
			}
			pos = np + vlen
			// context[4] open (value) OR context[5] open (propertyAccessError)
			tn, ctx, isOpen, _, _, np, err = readTagHdr(data, pos)
			if err != nil || !ctx || !isOpen {
				return out
			}
			pos = np
			if tn == 5 {
				out = append(out, PropResult{ObjType: objType, Instance: inst, Prop: prop, OK: false})
				pos = skipToClose(data, pos, 5)
			} else {
				if v, _, ok := decodeAppValue(data, pos); ok {
					out = append(out, PropResult{ObjType: objType, Instance: inst, Prop: prop, Value: v, OK: true})
				}
				pos = skipToClose(data, pos, 4)
			}
			// consume context[2] closing
			tn, ctx, _, isClose, _, np, err = readTagHdr(data, pos)
			if err == nil && ctx && tn == 2 && isClose {
				pos = np
			}
		}
	}
	return out
}

// skipToClose advances past data until the closing context tag `ctx` is consumed.
func skipToClose(data []byte, pos, ctx int) int {
	for pos < len(data) {
		tn, isCtx, isOpen, isClose, vlen, np, err := readTagHdr(data, pos)
		if err != nil {
			return len(data)
		}
		if isCtx && tn == ctx && isClose {
			return np
		}
		if isOpen {
			pos = np
			continue
		}
		pos = np + vlen
	}
	return pos
}

// decodeReadPropertyAck decodes a single-property ReadProperty ComplexAck and
// returns its scalar value.
func decodeReadPropertyAck(data []byte) (float64, bool) {
	// header: context[0] obj-id, context[1] prop-id, [context[2] index],
	// context[3] opening + value + closing.
	pos := 0
	for pos < len(data) {
		tn, ctx, isOpen, _, vlen, np, err := readTagHdr(data, pos)
		if err != nil {
			return 0, false
		}
		if ctx && tn == 3 && isOpen {
			if v, _, ok := decodeAppValue(data, np); ok {
				return v, true
			}
			return 0, false
		}
		if isOpen {
			pos = np
			continue
		}
		pos = np + vlen
	}
	return 0, false
}

// decodeIAm returns the device instance from an I-Am APDU payload.
func decodeIAm(data []byte) (int, bool) {
	if len(data) >= 5 && data[0] == 0xC4 {
		_, inst := decodeOID(data[1:5])
		return inst, true
	}
	return 0, false
}

// covNotification is the parsed content of an Unconfirmed COV Notification.
type covNotification struct {
	deviceInstance int
	objType        int
	instance       int
	presentValue   float64
	hasValue       bool
}

// decodeCOVNotification parses an UnconfirmedCOVNotification APDU payload:
//
//	[0] subscriberProcessId, [1] initiatingDeviceId (oid),
//	[2] monitoredObjectId (oid), [3] timeRemaining,
//	[4] opening listOfValues { propertyId, [value] ... } closing.
func decodeCOVNotification(data []byte) (covNotification, bool) {
	var n covNotification
	var (
		tn, vlen, np int
		ctx, isOpen  bool
		isClose      bool
		err          error
	)
	pos := 0
	// context[0] process id
	_, _, _, _, vlen, np, err = readTagHdr(data, pos)
	if err != nil {
		return n, false
	}
	pos = np + vlen
	// context[1] initiating device id (oid)
	_, _, _, _, _, np, err = readTagHdr(data, pos)
	if err != nil || np+4 > len(data) {
		return n, false
	}
	_, n.deviceInstance = decodeOID(data[np : np+4])
	pos = np + 4
	// context[2] monitored object id (oid)
	_, _, _, _, _, np, err = readTagHdr(data, pos)
	if err != nil || np+4 > len(data) {
		return n, false
	}
	n.objType, n.instance = decodeOID(data[np : np+4])
	pos = np + 4
	// context[3] time remaining
	_, _, _, _, vlen, np, err = readTagHdr(data, pos)
	if err != nil {
		return n, false
	}
	pos = np + vlen
	// context[4] opening listOfValues
	tn, ctx, isOpen, _, _, np, err = readTagHdr(data, pos)
	if err != nil || !ctx || tn != 4 || !isOpen {
		return n, false
	}
	pos = np
	// each value entry mirrors the RPM result wrapping:
	//   context[2] open { context[0] propId, context[4] open <value> close } close
	for pos < len(data) {
		tn, ctx, isOpen, isClose, _, np, err = readTagHdr(data, pos)
		if err != nil {
			break
		}
		if ctx && tn == 4 && isClose { // end listOfValues
			break
		}
		if !(ctx && tn == 2 && isOpen) {
			break
		}
		pos = np
		// context[0] propertyIdentifier
		tn, ctx, _, _, vlen, np, err = readTagHdr(data, pos)
		if err != nil || !ctx || tn != 0 || np+vlen > len(data) {
			break
		}
		prop := 0
		for i := 0; i < vlen; i++ {
			prop = prop<<8 | int(data[np+i])
		}
		pos = np + vlen
		// context[4] open value
		tn, ctx, isOpen, _, _, np, err = readTagHdr(data, pos)
		if err == nil && ctx && tn == 4 && isOpen {
			pos = np
			if v, _, ok := decodeAppValue(data, pos); ok && prop == PropPresentValue {
				n.presentValue = v
				n.hasValue = true
			}
			pos = skipToClose(data, pos, 4)
		}
		// consume context[2] closing
		tn, ctx, _, isClose, _, np, err = readTagHdr(data, pos)
		if err == nil && ctx && tn == 2 && isClose {
			pos = np
		}
	}
	return n, n.hasValue
}
