package nsqlookupd

import (
	"github.com/youzan/nsq/consistence"
	"github.com/youzan/nsq/internal/levellogger"
)

var nsqlookupLog = levellogger.NewLevelLogger(1, &levellogger.SimpleLogger{})

func SetLogger(logger levellogger.Logger, level int32) {
	nsqlookupLog.Logger = logger
	nsqlookupLog.SetLevel(level)
	consistence.SetCoordLogger(logger, level)
}
