package nsqlookupd_migrate

import (
	"testing"
	"github.com/youzan/nsq/internal/context"
	"time"
	"github.com/absolute8511/glog"
	"io/ioutil"
	"os"
	"github.com/youzan/nsq/internal/log"
)

func TestNewTester(t *testing.T) {
	dir, _ := ioutil.TempDir("", "TestNewTester")
	defer os.RemoveAll(dir)

	glog.SetGLogDir(dir)
	glog.StartWorker(1*time.Second)
	context := &context.Context{
		TestClientNum: 1,
		Test:true,
		ProxyHttpAddrTest:"http://127.0.0.1:4161",
		LogLevel:2,
	}
	tester, err := NewTester(context)
	if err != nil {
		t.FailNow()
	}
	tester.Start()
	<-time.After(5 * time.Second)
	tester.Close()
}

func TestCalculateQps(t *testing.T) {
	dir, _ := ioutil.TempDir("", "TestCalculateQps")
	defer os.RemoveAll(dir)

	glog.SetGLogDir(dir)
	glog.StartWorker(1*time.Second)
	context := &context.Context{
		TestClientNum: 100,
		Test:true,
		ProxyHttpAddrTest:"http://127.0.0.1:4161",
		LogLevel:2,
	}
	tester, err := NewTester(context)
	if err != nil {
		t.FailNow()
	}
	tester.Start()
	<-time.After(30 * time.Second)
	t.Logf("access qps %v", tester.GetQps())
	tester.Close()
}

func BenchmarkCalculateQps10(b *testing.B) {benchmarkCalculateQps(b, 10)}
func BenchmarkCalculateQps20(b *testing.B) {benchmarkCalculateQps(b, 20)}
func BenchmarkCalculateQps40(b *testing.B) {benchmarkCalculateQps(b, 40)}
func BenchmarkCalculateQps80(b *testing.B) {benchmarkCalculateQps(b, 80)}
func BenchmarkCalculateQps160(b *testing.B) {benchmarkCalculateQps(b, 160)}

func benchmarkCalculateQps(b *testing.B, clientNum int64) {
	dir, _ := ioutil.TempDir("", "TestCalculateQps")
	defer os.RemoveAll(dir)

	glog.SetGLogDir(dir)
	glog.StartWorker(1*time.Second)
	context := &context.Context{
		TestClientNum: clientNum,
		Test:true,
		ProxyHttpAddr:"http://127.0.0.1:4161",
		LogLevel:2,
	}
	tester, err := NewTester(context)
	if err != nil {
		b.FailNow()
	}
	tester.Start()
	b.StartTimer()
	<-time.After(30 * time.Second)
	b.Logf("access qps %v", tester.GetQps())
	b.StopTimer()
	tester.Close()
}

/**
http-address = "0.0.0.0:4171"
origin-lookupd-http = "http://127.0.0.2:4161"
target-lookupd-http = "http://127.0.0.1:4161"
dcc-url = "http://10.9.7.75:8089"
dcc-backup-file = "/tmp/nsqlookupd_migrate/backup"
env = "qa"
log-level = 2
log-dir = "/data/logs/nsqlookup_migrate"
migrate-dcc-eky = "nsq-dev.to.sqs-qa"
test=true
http-address-test = "http://127.0.0.1:4161"
dcc-test=false
 */
func TestMCTester(t *testing.T) {
	dir, _ := ioutil.TempDir("", "TestMCTester")
	defer os.RemoveAll(dir)

	glog.SetGLogDir(dir)
	glog.StartWorker(1*time.Second)
	context := &context.Context{
		TestClientNum: 100,
		Test:true,
		ProxyHttpAddrTest:"http://127.0.0.1:4161",
		LogLevel:2,
		DccUrl:"http://10.9.7.75:8089",
		DccBackupFile:dir,
		DCC_key:"nsq-dev.to.sqs-qa",
		Logger:log.NewMigrateLogger(2),
	}
	NewTester(context)
	mcTester, err :=  NewMCTester(context)
	if err != nil {
		t.FailNow()
	}
	mcTester.Start()
	<-time.After(10*time.Second)
	mcTester.Close()
}
