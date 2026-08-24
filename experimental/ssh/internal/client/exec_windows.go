//go:build windows

package client

import "errors"

// execProcess is unsupported on Windows: syscall.Exec has no Windows equivalent.
// The agent shim only ever runs on the (Linux) serverless driver, so this stub
// exists solely to keep the package building on Windows.
func execProcess(argv0 string, argv, env []string) error {
	return errors.New("process exec is not supported on Windows")
}
