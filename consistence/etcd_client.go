package consistence

import (
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"golang.org/x/net/context"
)

type EtcdClient struct {
	client client.Client
	kapi   client.KeysAPI
}

func NewEClient(host string) (*EtcdClient, error) {
	machines := strings.Split(host, ",")
	initEtcdPeers(machines)
	cfg := client.Config{
		Endpoints:               machines,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: time.Second,
	}

	c, err := client.New(cfg)
	if err != nil {
		return nil, err
	}

	return &EtcdClient{
		client: c,
		kapi:   client.NewKeysAPI(c),
	}, nil
}

func (self *EtcdClient) GetNewest(key string, sort, recursive bool) (*client.Response, error) {
	getOptions := &client.GetOptions{
		Recursive: recursive,
		Sort:      sort,
		Quorum:    true,
	}
	return self.kapi.Get(context.Background(), key, getOptions)
}

func (self *EtcdClient) Get(key string, sort, recursive bool) (*client.Response, error) {
	getOptions := &client.GetOptions{
		Recursive: recursive,
		Sort:      sort,
	}
	return self.kapi.Get(context.Background(), key, getOptions)
}

func (self *EtcdClient) Create(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		PrevExist: client.PrevNoExist,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) Delete(key string, recursive bool) (*client.Response, error) {
	delOptions := &client.DeleteOptions{
		Recursive: recursive,
	}
	return self.kapi.Delete(context.Background(), key, delOptions)
}

func (self *EtcdClient) CreateDir(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
		Dir: true,
	}
	return self.kapi.Set(context.Background(), key, "", setOptions)
}

func (self *EtcdClient) CreateInOrder(dir string, value string, ttl uint64) (*client.Response, error) {
	cirOptions := &client.CreateInOrderOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	return self.kapi.CreateInOrder(context.Background(), dir, value, cirOptions)
}

func (self *EtcdClient) Set(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL: time.Duration(ttl) * time.Second,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) SetWithTTL(key string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   true,
		PrevExist: client.PrevExist,
	}
	return self.kapi.Set(context.Background(), key, "", setOptions)
}

func (self *EtcdClient) Update(key string, value string, ttl uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		TTL:       time.Duration(ttl) * time.Second,
		Refresh:   true,
		PrevExist: client.PrevExist,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) CompareAndSwap(key string, value string, ttl uint64, prevValue string, prevIndex uint64) (*client.Response, error) {
	setOptions := &client.SetOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
		TTL:       time.Duration(ttl) * time.Second,
	}
	return self.kapi.Set(context.Background(), key, value, setOptions)
}

func (self *EtcdClient) CompareAndDelete(key string, prevValue string, prevIndex uint64) (*client.Response, error) {
	delOptions := &client.DeleteOptions{
		PrevValue: prevValue,
		PrevIndex: prevIndex,
	}
	return self.kapi.Delete(context.Background(), key, delOptions)
}

func (self *EtcdClient) Watch(key string, waitIndex uint64, recursive bool) client.Watcher {
	watchOptions := &client.WatcherOptions{
		AfterIndex: waitIndex,
		Recursive:  recursive,
	}
	return self.kapi.Watcher(key, watchOptions)
}
