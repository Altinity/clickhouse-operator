from testflows.core import *
from testflows.connect import Shell


@TestStep(Given)
def get_shell(self, timeout=300):
    """Create shell terminal."""
    try:
        shell = Shell()
        shell.timeout = timeout
        yield shell
    finally:
        with Finally("I close shell"):
            shell.close()


@TestStep(Given)
def add_kafka_config(self, timeout=300):
    """Add config with kafka configuration."""
    pass