package haunt_lock

import (
	"fmt"
	"strings"
)

const (
	LOG_DEFAULT = iota
	LOG_ERROR
	LOG_INFO
	LOG_DEBUG
)

type Logger interface {
	Output(maxdepth int, s string) error
	OutputErr(maxdepth int, s string) error
	OutputWarning(maxdepth int, s string) error
}

var logger *lockLogger

func SetLogger(l Logger, level int) {
	logger = &lockLogger{log: l, level: level}
}

type lockLogger struct {
	log   Logger
	level int
}

func (p *lockLogger) Info(args ...interface{}) {
	if p.log == nil {
		return
	}
	if p.level >= LOG_INFO {
		msg := "INFO: " + fmt.Sprint(args...)
		p.log.Output(2, msg)
	}
}

func (p *lockLogger) Infof(f string, args ...interface{}) {
	if p.log == nil {
		return
	}

	if p.level >= LOG_INFO {
		msg := "INFO: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.Output(2, msg)
	}
}

func (p *lockLogger) Error(args ...interface{}) {
	if p.log == nil {
		return
	}

	if p.level >= LOG_INFO {
		msg := "ERROR: " + fmt.Sprint(args...)
		p.log.OutputErr(2, msg)
	}
}

func (p *lockLogger) Errorf(f string, args ...interface{}) {
	if p.log == nil {
		return
	}

	if p.level >= LOG_INFO {
		msg := "ERROR: " + fmt.Sprintf(f, args...)
		// Append newline if necessary
		if !strings.HasSuffix(msg, "\n") {
			msg = msg + "\n"
		}
		p.log.OutputErr(2, msg)
	}
}

func init() {
	// Default logger uses the go default log.
	SetLogger(nil, LOG_INFO)
}
