package consistence

import (
	"github.com/youzan/nsq/internal/levellogger"
)

var coordLog = levellogger.NewLevelLogger(levellogger.LOG_INFO, nil)

func SetCoordLogger(log levellogger.Logger, level int32) {
	coordLog.Logger = log
	coordLog.SetLevel(level)
}

func SetCoordLogLevel(level int32) {
	coordLog.SetLevel(level)
}
