//        file: consistence/utils.go
// description: utils function of nsq etcd mgr

//      author: reezhou
//       email: reechou@gmail.com
//   copyright: youzan

package consistence

import (
	"net/url"
	"os"
	"strings"

	etcdlock "github.com/absolute8511/xlock2"
	"github.com/coreos/etcd/client"
)

var (
	hostname string
)

func NewEtcdClient(etcdHost string) *etcdlock.EtcdClient {
	machines := strings.Split(etcdHost, ",")
	initEtcdPeers(machines)
	if len(machines) == 1 && machines[0] == "" {
		machines[0] = "http://127.0.0.1:4001"
	}
	return etcdlock.NewEClient(etcdHost)
}

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
