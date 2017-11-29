package topic_migrate_manager

import (
	"testing"
	"github.com/DoraALin/docker/pkg/testutil/assert"
	"time"
	"github.com/youzan/nsq/internal/context"
)

func TestNewMigrateConfig(t *testing.T) {
	lookupdOri := "http://127.0.0.2:4161"
	lookupdTar := "http://127.0.0.1:4161"
	context := &context.Context{
		LookupAddrOri:lookupdOri,
		LookupAddrTar:lookupdTar,
		Env:"qa",
	}
	mCnf, error := NewMigrateConfig(context)
	if error != nil {
		t.Fatal("fail to create migrate config")
		t.FailNow()
	}
	assert.Equal(t, mCnf.App, "nsqlookupd.migrate")
	assert.Equal(t, mCnf.Env, "qa")
	assert.Equal(t, mCnf.Key, "nsq-dev.to.sqs-qa")
}


func TestNewTopicMigrateGuard(t *testing.T) {
	context := &context.Context{
		LookupAddrOri: "http://127.0.0.2:4161",
		LookupAddrTar: "http://127.0.0.1:4161",
		DccUrl: "http://10.9.7.75:8089",
		DccBackupFile: "backup.dat",
		Env: "qa",
	}
	mg, error := NewTopicMigrateGuard(context)
	if error != nil {
		t.Error("Fail to initialize topic migrate guard. %v", error)
	}
	mg.Init()
	time.Sleep(100*time.Millisecond)
}
