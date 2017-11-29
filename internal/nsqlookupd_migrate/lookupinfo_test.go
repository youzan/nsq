package nsqlookupd_migrate

import (
	"testing"
	"net/http"
	"encoding/json"
	"log"
)

func TestParseLookupInfoOld(t *testing.T) {
	resp, _ := http.Get("http://127.0.0.2:4161/lookup?topic=JavaTesting-Producer-Base&access=r")
	t.Logf("response code: %v", resp.StatusCode)
	defer resp.Body.Close()
	var lookupinfo Lookupinfo_old
	if err := json.NewDecoder(resp.Body).Decode(&lookupinfo); err != nil {
		mLog.Error("fail to parse lookup info, Err: %v", err)
	}
	log.Printf("%v", lookupinfo)
}