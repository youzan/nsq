package consistence

import (
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type EtcdClient struct {
	client  client.Client
	kapi    client.KeysAPI
	timeout time.Duration
}

var etcdTransport client.CancelableTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	Dial: (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 10 * time.Second,
	}).Dial,
	TLSHandshakeTimeout: 10 * time.Second,
	WriteBufferSize:     1024,
	ReadBufferSize:      1024,
}

func NewEClient(host, userName, pwd string) (*EtcdClient, error) {
	machines := strings.Split(host, ",")
	initEtcdPeers(machines)

	cfg := client.Config{
		Endpoints:               machines,
		Transport:               etcdTransport,
		HeaderTimeoutPerRequest: time.Second * 5,
		Username:                userName,
		Password:                pwd,
	}

	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	return &EtcdClient{
		client:  c,
		kapi:    client.NewKeysAPI(c),
		timeout: time.Second * 10,
	}, nil
}

func (self *EtcdClient) GetNewest(key string, sort, recursive bool) (*client.Response, error) {
	getOptions := &client.GetOptions{
		Recursive: recursive,
		Sort:      sort,
		Quorum:    true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Get(ctx, key, getOptions)
}

func (self *EtcdClient) Get(key string, sort, recursive bool) (*client.Response, error) {
	getOptions := &client.GetOptions{
		Recursive: recursive,
		Sort:      sort,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Get(ctx, key, getOptions)
}

func (self *EtcdClient) Create(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		PrevExist: client.PrevNoExist,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, value, setOptions)
}

func (self *EtcdClient) Delete(key string, recursive bool) (*client.Response, error) {
	delOptions := &client.DeleteOptions{
		Recursive: recursive,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Delete(ctx, key, delOptions)
}

func (self *EtcdClient) CreateDir(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
		Dir: true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, "", setOptions)
}

func (self *EtcdClient) CreateInOrder(dir string, value string, ttl uint64) (*client.Response, error) {
	cirOptions := &client.CreateInOrderOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.CreateInOrder(ctx, dir, value, cirOptions)
}

func (self *EtcdClient) Set(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, value, setOptions)
}

func (self *EtcdClient) SetWithTTL(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   true,
		PrevExist: client.PrevExist,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, "", setOptions)
}

func (self *EtcdClient) Update(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   true,
		PrevExist: client.PrevExist,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, value, setOptions)
}

func (self *EtcdClient) CompareAndSwap(key string, value string, ttl uint64, prevValue string, prevIndex uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
		TTL:       time.Duration(ttl) * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Set(ctx, key, value, setOptions)
}

func (self *EtcdClient) CompareAndDelete(key string, prevValue string, prevIndex uint64) (*client.Response, error) {
	delOptions := &client.DeleteOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
	}
	ctx, cancel := context.WithTimeout(context.Background(), self.timeout)
	defer cancel()
	return self.kapi.Delete(ctx, key, delOptions)
}

func (self *EtcdClient) Watch(key string, waitIndex uint64, recursive bool) client.Watcher {
	watchOptions := &client.WatcherOptions{
		AfterIndex: waitIndex,
		Recursive:  recursive,
	}
	return self.kapi.Watcher(key, watchOptions)
}
