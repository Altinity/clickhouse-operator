package chi

import (
	"fmt"
	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

func NewShellCommandWrapper(c string) *ShellCommandWrapper {
	cmd := exec.Command("/bin/bash", "-c", c)
	return &ShellCommandWrapper{Cmd: cmd}
}

type ShellCommandWrapper struct {
	*exec.Cmd
}

func (c *ShellCommandWrapper) print(s string, level int, depth int) {
	_, file, line, ok := runtime.Caller(depth)
	prefix := "???]"
	if ok {
		prefix = fmt.Sprintf("%s:%d]", filepath.Base(file), line)
	}
	log.V(1).Info("%s %s", prefix, s)
}

func (c *ShellCommandWrapper) Run() error {
	c.print(c.String(), 2, 3)
	return c.Cmd.Run()
}

func (c *ShellCommandWrapper) CombinedOutput() (string, error) {
	c.print(c.String(), 4, 3)
	o, err := c.Cmd.CombinedOutput()
	outputString := string(o)
	if err != nil {
		return outputString, err
	}
	return outputString, nil
}

func Shell(envs map[string]string, stdout io.Writer, c string, a ...interface{}) (string, error) {
	if len(a) > 0 {
		c = fmt.Sprintf(c, a...)
	}
	cmd := NewShellCommandWrapper(c)
	for key, value := range envs {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
	}
	if stdout == nil {
		return cmd.CombinedOutput()
	}
	cmd.Stdout = stdout
	cmd.Stderr = os.Stderr
	return "", cmd.Run()
}

func SyncUsers(namespace, srcPodName, dstPodName, containerName, dirPath string) error {
	baseArgs := fmt.Sprintf("-n %s -c %s", namespace, containerName)
	command := fmt.Sprintf("/kubectl exec %s %s  -- tar cf - %s /tmp/ | /kubectl exec %s -i %s -- tar xvf - -C /", baseArgs, srcPodName, dirPath, baseArgs, dstPodName)
	out, err := Shell(nil, nil, command)
	if err != nil {
		return err
	}
	log.V(1).Info(out)
	return nil
}
