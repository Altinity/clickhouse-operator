import os
import time
import inspect
import threading

import testflows.settings as settings

from testflows.core import *
from testflows.asserts import error
from testflows.connect import Shell as ShellBase


class Shell(ShellBase):
    timeout = 600

    def __exit__(self, type, value, traceback):
        # send exit and Ctrl-D repeatedly
        # to terminate any open shell commands.
        # This is needed for example
        # to solve a problem with
        # 'docker-compose exec {name} bash --noediting'
        # that does not clean up open bash processes
        # if not exited normally
        for i in range(10):
            if self.child is not None:
                try:
                    self.send("exit\r", eol="")
                    self.send("\x04\r", eol="")
                except OSError:
                    pass
        return super(Shell, self).__exit__(type, value, traceback)


class Cluster(object):
    """Simple object around docker-compose cluster."""

    def __init__(self, configs_dir=None):

        self.environ = {}
        self.configs_dir = configs_dir
        self.docker_compose = "docker-compose"
        self.shell = Shell()

        frame = inspect.currentframe().f_back
        caller_dir = os.path.dirname(os.path.abspath(frame.f_globals["__file__"]))

        # auto set configs directory
        if self.configs_dir is None:
            caller_configs_dir = caller_dir
            if os.path.exists(caller_configs_dir):
                self.configs_dir = caller_configs_dir

        if not os.path.exists(self.configs_dir):
            raise TypeError(f"configs directory '{self.configs_dir}' does not exist")

        docker_compose_project_dir = os.path.join(caller_dir, "docker-compose")

        docker_compose_file_path = os.path.join(docker_compose_project_dir or "", "docker-compose.yml")

        self.docker_compose += (
            f' --project-directory "{docker_compose_project_dir}" --file "{docker_compose_file_path}"'
        )

    def __enter__(self):
        with Given("docker-compose cluster"):
            self.up()
        return self

    def __exit__(self, type, value, traceback):
        with Finally("I clean up"):
            self.down()

    def down(self, timeout=3600):
        """Bring cluster down by executing docker-compose down."""
        return self.shell(f"{self.docker_compose} down --timeout {timeout}  -v --remove-orphans")

    def up(self, timeout=3600):
        with Given("docker-compose"):
            max_attempts = 5
            max_up_attempts = 1

            for attempt in range(max_attempts):
                with When(f"attempt {attempt}/{max_attempts}"):
                    with By("checking if any containers are already running"):
                        self.shell(f"set -o pipefail && {self.docker_compose} ps | tee")

                    with And("executing docker-compose down just in case it is up"):
                        cmd = self.shell(
                            f"set -o pipefail && {self.docker_compose} down --timeout={timeout} -v --remove-orphans 2>&1 | tee"
                        )
                        if cmd.exitcode != 0:
                            continue

                    with And("checking if any containers are still left running"):
                        self.shell(f"set -o pipefail && {self.docker_compose} ps | tee")

                    with And("executing docker-compose up"):
                        for up_attempt in range(max_up_attempts):
                            with By(f"attempt {up_attempt}/{max_up_attempts}"):
                                cmd = self.shell(
                                    f"set -o pipefail && {self.docker_compose} up --force-recreate --timeout {timeout} -d 2>&1 | tee"
                                )
                                if "is unhealthy" not in cmd.output:
                                    break

                    with Then("check there are no unhealthy containers"):
                        ps_cmd = self.shell(f'set -o pipefail && {self.docker_compose} ps | tee | grep -v "Exit 0"')
                        if "is unhealthy" in cmd.output or "Exit" in ps_cmd.output:
                            self.shell(f"set -o pipefail && {self.docker_compose} logs | tee")
                            continue

                    if cmd.exitcode == 0 and "is unhealthy" not in cmd.output and "Exit" not in ps_cmd.output:
                        break

            if cmd.exitcode != 0 or "is unhealthy" in cmd.output or "Exit" in ps_cmd.output:
                fail("could not bring up docker-compose cluster")
