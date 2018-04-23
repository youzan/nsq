package nsq_sync

import "github.com/youzan/nsq/internal/levellogger"

var nsqSyncLog = levellogger.NewLevelLogger(1, &levellogger.SimpleLogger{})

func SetLogger(logger levellogger.Logger, level int32) {
	nsqSyncLog.Logger = logger
	nsqSyncLog.SetLevel(level)
}

