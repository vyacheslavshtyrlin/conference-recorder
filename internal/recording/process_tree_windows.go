//go:build windows

package recording

import (
	"context"
	"os/exec"
	"strconv"
	"syscall"
)

func configureProcessTree(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP, HideWindow: true}
}

func killProcessTree(ctx context.Context, command *exec.Cmd) {
	if command.Process == nil {
		return
	}
	treeKill := exec.CommandContext(
		ctx,
		"taskkill",
		"/PID",
		strconv.Itoa(command.Process.Pid),
		"/T",
		"/F",
	)
	treeKill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	_ = treeKill.Run()
	_ = command.Process.Kill()
}
