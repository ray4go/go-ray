package ray

import (
	"encoding/binary"
)

// decodeBytesUnits decodes byte units from a single bytes.
// Each unit has the format: | length:8byte:int64 | data:${length}byte:[]byte |
// It returns a slice of bytes, where each bytes is a data unit.
func decodeBytesUnits(data []byte) ([][]byte, bool) {
	var units [][]byte
	offset := 0

	// Loop as long as there's potentially a full unit left.
	for offset < len(data) {
		// 1. Decode length prefix (8 bytes)
		// Check if there are enough bytes for the length prefix.
		if len(data) < offset+8 {
			// Not enough data for a length prefix, so we stop.
			break
		}

		// Convert 8 bytes to an integer. We use LittleEndian
		length := int(binary.LittleEndian.Uint64(data[offset : offset+8]))

		// Move offset past the length prefix.
		offset += 8

		// 2. Decode data part
		// Check if the slice contains the full data unit as declared by the length.
		if len(data) < offset+length {
			// The data is truncated; the declared length exceeds the remaining bytes.
			break
		}

		// Extract the data unit.
		unit := data[offset : offset+length]
		units = append(units, unit)

		// Move offset to the beginning of the next unit.
		offset += length
	}
	return units, offset == len(data)
}

func copyMap(src map[int][]byte) map[int][]byte {
	if src == nil {
		return nil
	}
	dst := make(map[int][]byte, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
