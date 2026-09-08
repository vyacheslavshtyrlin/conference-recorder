package recording

import "github.com/pion/rtp"

const maximumH264ParameterSetBytes = 64 * 1024

type h264Fragment struct {
	nalType      byte
	nextSequence uint16
	payload      []byte
}

type h264ParameterSets struct {
	sps      []byte
	pps      []byte
	fragment *h264Fragment
}

func (sets *h264ParameterSets) Observe(packet *rtp.Packet) {
	if packet == nil || len(packet.Payload) == 0 {
		sets.fragment = nil
		return
	}
	payload := packet.Payload
	switch payload[0] & 0x1f {
	case 7, 8:
		sets.fragment = nil
		sets.store(payload)
	case 24:
		sets.fragment = nil
		sets.observeSTAPA(payload)
	case 28:
		sets.observeFUA(packet.SequenceNumber, payload)
	default:
		sets.fragment = nil
	}
}

func (sets *h264ParameterSets) Complete() bool {
	return len(sets.sps) > 0 && len(sets.pps) > 0
}

func (sets *h264ParameterSets) Prefix(reference *rtp.Packet) []*rtp.Packet {
	if !sets.Complete() || reference == nil {
		return []*rtp.Packet{}
	}
	return []*rtp.Packet{
		parameterSetPacket(reference, reference.SequenceNumber-2, sets.sps),
		parameterSetPacket(reference, reference.SequenceNumber-1, sets.pps),
	}
}

func (sets *h264ParameterSets) observeSTAPA(payload []byte) {
	units := [][]byte{}
	for offset := 1; offset+2 <= len(payload); {
		size := int(payload[offset])<<8 | int(payload[offset+1])
		offset += 2
		if size < 1 || offset+size > len(payload) {
			return
		}
		units = append(units, payload[offset:offset+size])
		offset += size
		if offset == len(payload) {
			for _, unit := range units {
				sets.store(unit)
			}
			return
		}
	}
}

func (sets *h264ParameterSets) observeFUA(sequence uint16, payload []byte) {
	if len(payload) < 3 {
		sets.fragment = nil
		return
	}
	nalType := payload[1] & 0x1f
	if nalType != 7 && nalType != 8 {
		sets.fragment = nil
		return
	}
	start := payload[1]&0x80 != 0
	end := payload[1]&0x40 != 0
	if start {
		if len(payload)-1 > maximumH264ParameterSetBytes {
			sets.fragment = nil
			return
		}
		nalHeader := payload[0]&0xe0 | nalType
		sets.fragment = &h264Fragment{
			nalType:      nalType,
			nextSequence: sequence + 1,
			payload:      append([]byte{nalHeader}, payload[2:]...),
		}
		if end {
			sets.store(sets.fragment.payload)
			sets.fragment = nil
		}
		return
	}
	if sets.fragment == nil || sets.fragment.nalType != nalType || sets.fragment.nextSequence != sequence {
		sets.fragment = nil
		return
	}
	if len(sets.fragment.payload)+len(payload)-2 > maximumH264ParameterSetBytes {
		sets.fragment = nil
		return
	}
	sets.fragment.payload = append(sets.fragment.payload, payload[2:]...)
	sets.fragment.nextSequence = sequence + 1
	if end {
		sets.store(sets.fragment.payload)
		sets.fragment = nil
	}
}

func (sets *h264ParameterSets) store(payload []byte) {
	if len(payload) == 0 || len(payload) > maximumH264ParameterSetBytes {
		return
	}
	copyPayload := append([]byte{}, payload...)
	switch payload[0] & 0x1f {
	case 7:
		sets.sps = copyPayload
	case 8:
		sets.pps = copyPayload
	}
}

func parameterSetPacket(reference *rtp.Packet, sequence uint16, payload []byte) *rtp.Packet {
	packet := *reference
	packet.Header = reference.Header
	packet.SequenceNumber = sequence
	packet.Marker = false
	packet.Payload = append([]byte{}, payload...)
	return &packet
}
