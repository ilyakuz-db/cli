//go:build !windows

package client

import "syscall"

// execProcess replaces the current process image with argv0, mirroring execve(2).
// The agent shim uses this so the ssh PTY connects straight to Claude Code and the
// child's exit code propagates without an intervening CLI process.
func execProcess(argv0 string, argv, env []string) error {
	return syscall.Exec(argv0, argv, env)
}
