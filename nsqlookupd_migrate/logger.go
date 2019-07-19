package nsqlookupd_migrate

import (
	"fmt"
	"sync/atomic"

	"github.com/absolute8511/glog"
)

const (
	LOG_ERR int32 = iota
	LOG_WARN
	LOG_INFO
	LOG_DEBUG
)

type MigrateLogger struct {
	level int32
}

func (logger *MigrateLogger) SetLevel(level int32) {
	atomic.StoreInt32(&logger.level, level)
	glog.Infof("Log level set to %v", level)
}

func (logger *MigrateLogger) Level() int32 {
	return atomic.LoadInt32(&logger.level)
}

func (logger *MigrateLogger) AccessTrace(format string, v ...interface{}) {
	if logger.Level() >= LOG_INFO {
		accessFmt := fmt.Sprintf("[access] %v", format)
		glog.InfoDepth(2, fmt.Sprintf(accessFmt, v...))
	}
}

func (logger *MigrateLogger) Debug(format string, v ...interface{}) {
	if logger.Level() >= LOG_DEBUG {
		glog.InfoDepth(2, fmt.Sprintf(format, v...))
	}
}

func (logger *MigrateLogger) Info(format string, v ...interface{}) {
	if logger.Level() >= LOG_INFO {
		glog.InfoDepth(2, fmt.Sprintf(format, v...))
	}
}

func (logger *MigrateLogger) Warn(format string, v ...interface{}) {
	glog.WarningDepth(2, fmt.Sprintf(format, v...))
}

func (logger *MigrateLogger) Error(format string, v ...interface{}) {
	glog.ErrorDepth(2, fmt.Sprintf(format, v...))
}

func NewMigrateLogger(level int32) *MigrateLogger {
	mLog := MigrateLogger{
		level,
	}
	return &mLog
}
