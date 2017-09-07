package nsqadmin

import (
	"github.com/youzan/nsq/internal/levellogger"
)

var adminLog = levellogger.NewLevelLogger(levellogger.LOG_INFO, &levellogger.GLogger{})
