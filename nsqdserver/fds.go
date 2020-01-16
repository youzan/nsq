// +build !linux

package nsqdserver

import (
	"fmt"
	"runtime"
)

// FDLimit get the open file limit
func FDLimit() (uint64, error) {
	return 0, fmt.Errorf("cannot get FDLimit on %s", runtime.GOOS)
}
