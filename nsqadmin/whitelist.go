package nsqadmin

import (
	"io/ioutil"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type AccessControl interface {
	IsAdmin(username string) bool
	Start()
	Stop()
}

type YamlAccessControl struct {
	filePath     string
	accessMap    map[string]interface{}
	lock         sync.RWMutex
	updateTicker *time.Ticker
	tStopChan    chan int
	wg           sync.WaitGroup
	lastUpdated  time.Time
	ctx          *Context
}

func readYml(path string) (map[string]interface{}, error) {
	buf, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	m := make(map[string]interface{})
	err = yaml.Unmarshal(buf, &m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func NewYamlAccessControl(ctx *Context, filePath string) (AccessControl, error) {
	if filePath == "" {
		return nil, nil
	}
	acMp, err := readYml(filePath)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, err
	}
	mt := info.ModTime()

	return &YamlAccessControl{
		filePath:    filePath,
		accessMap:   acMp,
		lastUpdated: mt,
		ctx:         ctx,
	}, nil
}

func (ac *YamlAccessControl) Start() {
	ac.tStopChan = make(chan int, 1)
	ac.updateTicker = time.NewTicker(10 * time.Second)
	ac.wg.Add(1)
	go func() {
		defer ac.wg.Done()
		defer ac.updateTicker.Stop()
		for {
			select {
			case <-ac.updateTicker.C:
				{
					//update access control map
					ac.tryUpdateControlFile()
				}
			case <-ac.tStopChan:
				{
					return
				}
			}
		}
	}()
}

func (ac *YamlAccessControl) Stop() {
	close(ac.tStopChan)
	ac.wg.Wait()
}

func (ac *YamlAccessControl) IsAdmin(username string) bool {
	ac.lock.RLock()
	defer ac.lock.RUnlock()
	//anyone is admin if access control file is disabled
	if !ac.isEnable() {
		return true
	}
	if admins, exist := ac.accessMap["admin"]; !exist {
		return false
	} else {
		//ac.ctx.nsqadmin.logf("admins map %v", admins)
		if isAdmin, exist := admins.(map[string]interface{})[username]; !exist {
			return false
		} else {
			return isAdmin.(bool)
		}
	}
}

//read action in access control file DO NOT add read lock
func (ac *YamlAccessControl) isEnable() bool {
	if enable, exist := ac.accessMap["enable"]; !exist {
		return false
	} else {
		return enable.(bool)
	}
}

func (ac *YamlAccessControl) tryUpdateControlFile() {
	info, err := os.Stat(ac.filePath)
	if err != nil {
		ac.ctx.nsqadmin.logf("ERROR: fail to fetch info for access control file: %v", ac.filePath)
		return
	}
	mt := info.ModTime()
	//compare with last modified time
	if mt.After(ac.lastUpdated) {
		ac.ctx.nsqadmin.logf("INFO: access control file modified, try updating")
		newAcMap, err := readYml(ac.filePath)
		if err != nil {
			ac.ctx.nsqadmin.logf("ERROR: fail to read access control file from %v", ac.filePath)
			return
		}
		ac.lock.Lock()
		defer ac.lock.Unlock()
		ac.accessMap = newAcMap
		ac.lastUpdated = mt
	}
}
