package nsqlookupd_migrate

import (
	"strings"
	"errors"
	"fmt"
	"regexp"
)

type MigrateConfig struct {
	App	string
	Key	string
	ReqType string
	Env     string
	Switches   map[string]int
}

var cnfLog *MigrateLogger
var keyFormat = "%s.to.%s"
var appFormat = "nsqlookupd.migrate"
var typeFormat = "COMBINATION"
var IP_REGEXP = "(\\d{1,3}\\.){3}\\d{1,3}(:\\d{1,5})?"
func parseClusterName(lookupdAddr string) string {
	match, _ := regexp.MatchString(IP_REGEXP, lookupdAddr)
	if match == true {
		re := regexp.MustCompile(IP_REGEXP)
		return re.FindString(lookupdAddr)
	} else {
		return strings.Split(strings.Split(lookupdAddr, ".")[0], "http://")[1]
	}
}

func initKey(originLookupd, targetLookupd, key_in_cnf string) (string, error) {
	var hostOri, hostTar string
	if key_in_cnf != "" {
		cnfLog.Info("key in config applied: %v", key_in_cnf)
		return key_in_cnf, nil
	}
	if hostOri = parseClusterName(originLookupd); hostOri == "" {
		msg := fmt.Sprintf("Fail to parse NSQ cluster id from %v.", originLookupd)
		cnfLog.Error(msg)
		return "", errors.New(msg)
	}

	if hostTar = parseClusterName(targetLookupd); hostTar == "" {
		msg := fmt.Sprintf("Fail to parse NSQ cluster id from %v.", targetLookupd)
		cnfLog.Error(msg)
		return "", errors.New(msg)
	}

	return fmt.Sprintf(keyFormat, hostOri, hostTar), nil
}

/**
init and answer a new migrate config with no topic switches
 */
func NewMigrateConfig(context *Context) (*MigrateConfig, error) {
	cnfLog = context.Logger
	app := appFormat
	key, err := initKey(context.LookupAddrOri, context.LookupAddrTar, context.Migrate_key)
	if err != nil {
		return nil, err
	}
	typeVal := typeFormat
	cnfLog.Info("New migrate config initialized: %v, %v, %v, %v", app, key, typeVal, context.Env)
	return &MigrateConfig{
		App: app,
		Key: key,
		ReqType: typeVal,
		Env: context.Env,
		Switches: make(map[string]int),
	}, nil
}

