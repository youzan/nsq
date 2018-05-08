//        file: haunt_lock/haunt_lock_utils.go
// description: Utility for haunt lock of etcd.

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package haunt_lock

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"sync/atomic"
	
	"github.com/coreos/etcd/client"
)

const (
	ErrCodeEtcdNotReachable    = 501
	ErrCodeUnhandledHTTPStatus = 502
)

var (
	lockID   int64
	pid      int
	hostname string
	ip       string
)

func initEtcdPeers(machines []string) error {
	for i, ep := range machines {
		u, err := url.Parse(ep)
		if err != nil {
			return err
		}
		if u.Scheme == "" {
			u.Scheme = "http"
		}
		machines[i] = u.String()
	}
	return nil
}

func IsEtcdNotReachable(err error) bool {
	if cErr, ok := err.(client.Error); ok {
		return cErr.Code == ErrCodeEtcdNotReachable
	}
	return false
}

func IsEtcdWatchExpired(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeEventIndexCleared)
}

func isEtcdErrorNum(err error, errorCode int) bool {
	if err != nil {
		if etcdError, ok := err.(client.Error); ok {
			return etcdError.Code == errorCode
		}
	}
	return false
}

func GetMasterDir() string {
	return HAUNT_MASTER_DIR
}

func getID() string {
	return fmt.Sprintf("%d:%d:%s:%s", atomic.AddInt64(&lockID, 1), pid, hostname, ip)
}

func init() {
	lockID = 0
	pid = os.Getpid()
	hostName, err := os.Hostname()
	if err != nil {
		fmt.Println("Get hostname error:", err.Error())
		return
	}
	hostname = hostName
	ipAddress, err := net.ResolveIPAddr("ip", hostname)
	if err != nil {
		fmt.Println("Get IPAddress error:", err.Error())
		return
	}
	ip = ipAddress.String()
}
