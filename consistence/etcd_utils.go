package consistence

import (
	"net/url"
	"os"

	"github.com/coreos/etcd/client"
)

const (
	ErrCodeEtcdNotReachable    = 501
	ErrCodeUnhandledHTTPStatus = 502
)

var (
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

func CheckKeyIfExist(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeKeyNotFound)
}

func IsEtcdNotFile(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeNotFile)
}

func IsEtcdNodeExist(err error) bool {
	return isEtcdErrorNum(err, client.ErrorCodeNodeExist)
}

func isEtcdErrorNum(err error, errorCode int) bool {
	if err != nil {
		if etcdError, ok := err.(client.Error); ok {
			return etcdError.Code == errorCode
		}
		// NOTE: There are other error types returned
	}
	return false
}

func init() {
	hostname, _ = os.Hostname()
}
