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
// limitations under the License.package rockredis

package codec

import (
	"errors"
	"fmt"
)

const (
	NilFlag   byte = 0
	bytesFlag byte = 1
	intFlag   byte = 3
	uintFlag  byte = 4
	floatFlag byte = 5
	maxFlag   byte = 250
)

func memcmpEncode(b []byte, vals []interface{}) ([]byte, error) {
	for _, val := range vals {
		switch realVal := val.(type) {
		case byte:
			b = encodeInt64(b, int64(realVal))
		case int8:
			b = encodeInt64(b, int64(realVal))
		case int16:
			b = encodeInt64(b, int64(realVal))
		case int:
			b = encodeInt64(b, int64(realVal))
		case int64:
			b = encodeInt64(b, int64(realVal))
		case int32:
			b = encodeInt64(b, int64(realVal))
		case uint16:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(realVal))
		case uint32:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(realVal))
		case uint:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(realVal))
		case uint64:
			b = append(b, uintFlag)
			b = EncodeUint(b, uint64(realVal))
		case string:
			b = encodeBytes(b, []byte(realVal))
		case []byte:
			b = encodeBytes(b, realVal)
		case float32:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(realVal))
		case float64:
			b = append(b, floatFlag)
			b = EncodeFloat(b, float64(realVal))
		case nil:
			b = append(b, NilFlag)
		default:
			return nil, fmt.Errorf("unsupport encode type %v", realVal)
		}
	}
	return b, nil
}

func encodeBytes(b []byte, v []byte) []byte {
	b = append(b, bytesFlag)
	b = EncodeBytes(b, v)
	return b
}

func encodeInt64(b []byte, v int64) []byte {
	b = append(b, intFlag)
	b = EncodeInt(b, v)
	return b
}

// EncodeKey appends the encoded values to byte slice b, returns the appended
// slice. It guarantees the encoded value is in ascending order for comparison.
func EncodeMemCmpKey(b []byte, v ...interface{}) ([]byte, error) {
	return memcmpEncode(b, v)
}

func EncodeMaxKey(b []byte) ([]byte, error) {
	b = append(b, maxFlag)
	return b, nil
}

func EncodeMinNotNull(b []byte) ([]byte, error) {
	b = append(b, bytesFlag)
	return b, nil
}

// Decode decodes values from a byte slice generated with EncodeKey or EncodeValue
// before.
// size is the size of decoded slice.
func Decode(b []byte, size int) ([]interface{}, error) {
	if len(b) < 1 {
		return nil, errors.New("invalid encoded key")
	}

	var (
		err    error
		values = make([]interface{}, 0, size)
	)

	for len(b) > 0 {
		var d interface{}
		b, d, err = DecodeOne(b)
		if err != nil {
			return nil, err
		}
		values = append(values, d)
	}

	return values, nil
}

func DecodeOne(b []byte) (remain []byte, d interface{}, err error) {
	if len(b) < 1 {
		return nil, d, errors.New("invalid encoded key")
	}
	flag := b[0]
	b = b[1:]
	switch flag {
	case intFlag:
		var v int64
		b, v, err = DecodeInt(b)
		d = v
	case uintFlag:
		var v uint64
		b, v, err = DecodeUint(b)
		d = v
	case floatFlag:
		var v float64
		b, v, err = DecodeFloat(b)
		d = v
	case bytesFlag:
		var v []byte
		b, v, err = DecodeBytes(b)
		d = v
	case NilFlag:
	default:
		return b, d, fmt.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return b, d, err
	}
	return b, d, nil
}

func CutOne(b []byte) (data []byte, remain []byte, err error) {
	l, err := peek(b)
	if err != nil {
		return nil, nil, err
	}
	return b[:l], b[l:], nil
}

func peek(b []byte) (length int, err error) {
	if len(b) < 1 {
		return 0, errors.New("invalid encoded key")
	}
	flag := b[0]
	length++
	b = b[1:]
	var l int
	switch flag {
	case NilFlag:
	case intFlag, floatFlag:
		l = 8
	case bytesFlag:
		l, err = peekBytes(b, false)
	default:
		return 0, fmt.Errorf("invalid encoded key flag %v", flag)
	}
	if err != nil {
		return 0, err
	}
	length += l
	return
}

func peekBytes(b []byte, reverse bool) (int, error) {
	offset := 0
	for {
		if len(b) < offset+encGroupSize+1 {
			return 0, errors.New("insufficient bytes to decode value")
		}
		marker := b[offset+encGroupSize]
		var padCount byte
		if reverse {
			padCount = marker
		} else {
			padCount = encMarker - marker
		}
		offset += encGroupSize + 1
		// When padCount is not zero, it means we get the end of the byte slice.
		if padCount != 0 {
			break
		}
	}
	return offset, nil
}
