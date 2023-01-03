package version

import (
	"fmt"
	"runtime"
)

const Binary = "0.3.7-HA.1.12.9"

var (
	Commit    = "unset"
	BuildTime = "unset"
)

func String(app string) string {
	return fmt.Sprintf("%s v%s (built w/%s %s %s)", app, Binary, runtime.Version(), Commit, BuildTime)
}
