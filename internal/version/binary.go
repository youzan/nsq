package version

import (
	"fmt"
	"runtime"
)

const Binary = "0.3.7-HA.1.7.0"

func String(app string) string {
	return fmt.Sprintf("%s v%s (built w/%s)", app, Binary, runtime.Version())
}
