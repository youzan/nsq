package nsq

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
)

var (
	FailedOnNotLeader        = "E_FAILED_ON_NOT_LEADER"
	FailedOnNotLeaderBytes   = []byte("E_FAILED_ON_NOT_LEADER")
	E_TOPIC_NOT_EXIST        = "E_TOPIC_NOT_EXIST"
	E_TOPIC_NOT_EXIST_BYTES  = []byte("E_TOPIC_NOT_EXIST")
	FailedOnNotWritable      = "E_FAILED_ON_NOT_WRITABLE"
	FailedOnNotWritableBytes = []byte("E_FAILED_ON_NOT_WRITABLE")
)

func IsFailedOnNotLeader(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnNotLeader)
	}
	return false
}

func IsFailedOnNotWritable(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), FailedOnNotWritable)
	}
	return false
}

func IsTopicNotExist(err error) bool {
	if err != nil {
		return strings.HasPrefix(err.Error(), E_TOPIC_NOT_EXIST)
	}
	return false
}

func IsFailedOnNotLeaderBytes(err []byte) bool {
	if err != nil {
		return bytes.HasPrefix(err, FailedOnNotLeaderBytes)
	}
	return false
}

func IsTopicNotExistBytes(err []byte) bool {
	if err != nil {
		return bytes.HasPrefix(err, E_TOPIC_NOT_EXIST_BYTES)
	}
	return false
}

func IsFailedOnNotWritableBytes(err []byte) bool {
	if err != nil {
		return bytes.HasPrefix(err, FailedOnNotWritableBytes)
	}
	return false
}

// ErrNotConnected is returned when a publish command is made
// against a Producer that is not connected
var ErrNotConnected = errors.New("not connected")

// ErrStopped is returned when a publish command is
// made against a Producer that has been stopped
var ErrStopped = errors.New("stopped")

// ErrClosing is returned when a connection is closing
var ErrClosing = errors.New("closing")

// ErrAlreadyConnected is returned from ConnectToNSQD when already connected
var ErrAlreadyConnected = errors.New("already connected")

// ErrOverMaxInFlight is returned from Consumer if over max-in-flight
var ErrOverMaxInFlight = errors.New("over configure max-inflight")

// ErrIdentify is returned from Conn as part of the IDENTIFY handshake
type ErrIdentify struct {
	Reason string
}

// Error returns a stringified error
func (e ErrIdentify) Error() string {
	return fmt.Sprintf("failed to IDENTIFY - %s", e.Reason)
}

// ErrProtocol is returned from Producer when encountering
// an NSQ protocol level error
type ErrProtocol struct {
	Reason string
}

// Error returns a stringified error
func (e ErrProtocol) Error() string {
	return e.Reason
}
