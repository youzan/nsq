// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package codec

import (
	"encoding/binary"
	"errors"
	"math"
)

const signMask uint64 = 0x8000000000000000

func encodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask
}

func decodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

// EncodeInt appends the encoded value to slice b and returns the appended slice.
// EncodeInt guarantees that the encoded value is in ascending order for comparison.
func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := encodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// EncodeIntDesc appends the encoded value to slice b and returns the appended slice.
// EncodeIntDesc guarantees that the encoded value is in descending order for comparison.
func EncodeIntDesc(b []byte, v int64) []byte {
	var data [8]byte
	u := encodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], ^u)
	return append(b, data[:]...)
}

// DecodeInt decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeInt(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := decodeCmpUintToInt(u)
	b = b[8:]
	return b, v, nil
}

// DecodeIntDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeIntDesc(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := decodeCmpUintToInt(^u)
	b = b[8:]
	return b, v, nil
}

// EncodeUint appends the encoded value to slice b and returns the appended slice.
// EncodeUint guarantees that the encoded value is in ascending order for comparison.
func EncodeUint(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], v)
	return append(b, data[:]...)
}

// EncodeUintDesc appends the encoded value to slice b and returns the appended slice.
// EncodeUintDesc guarantees that the encoded value is in descending order for comparison.
func EncodeUintDesc(b []byte, v uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], ^v)
	return append(b, data[:]...)
}

// DecodeUint decodes value encoded by EncodeUint before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUint(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	v := binary.BigEndian.Uint64(b[:8])
	b = b[8:]
	return b, v, nil
}

// DecodeUintDesc decodes value encoded by EncodeInt before.
// It returns the leftover un-decoded slice, decoded value if no error.
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:8]
	v := binary.BigEndian.Uint64(data)
	b = b[8:]
	return b, ^v, nil
}

func encodeFloatToCmpUint64(f float64) uint64 {
	u := math.Float64bits(f)
	if f >= 0 {
		u |= signMask
	} else {
		u = ^u
	}
	return u
}

func decodeCmpUintToFloat(u uint64) float64 {
	if u&signMask > 0 {
		u &= ^signMask
	} else {
		u = ^u
	}
	return math.Float64frombits(u)
}

// EncodeFloat encodes a float v into a byte slice which can be sorted lexicographically later.
// EncodeFloat guarantees that the encoded value is in ascending order for comparison.
func EncodeFloat(b []byte, v float64) []byte {
	u := encodeFloatToCmpUint64(v)
	return EncodeUint(b, u)
}

// DecodeFloat decodes a float from a byte slice generated with EncodeFloat before.
func DecodeFloat(b []byte) ([]byte, float64, error) {
	b, u, err := DecodeUint(b)
	return b, decodeCmpUintToFloat(u), err
}

// EncodeFloatDesc encodes a float v into a byte slice which can be sorted lexicographically later.
// EncodeFloatDesc guarantees that the encoded value is in descending order for comparison.
func EncodeFloatDesc(b []byte, v float64) []byte {
	u := encodeFloatToCmpUint64(v)
	return EncodeUintDesc(b, u)
}

// DecodeFloatDesc decodes a float from a byte slice generated with EncodeFloatDesc before.
func DecodeFloatDesc(b []byte) ([]byte, float64, error) {
	b, u, err := DecodeUintDesc(b)
	return b, decodeCmpUintToFloat(u), err
}
