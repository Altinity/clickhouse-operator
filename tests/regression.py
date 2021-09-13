import sys
from testflows.core import *

from helpers.cluster import Cluster
from helpers.argparser import argparser

xfails = {
}


@TestSuite
@Name("clickhouse operator")
@XFails(xfails)
@ArgumentParser(argparser)
def regression(self, clickhouse_image, operator_version):
    """ClickHouse Operator test regression suite.
    """
    nodes = {"runners": ("runner", )}

    with Cluster(nodes=nodes) as cluster:
        self.context.cluster = cluster
        self.context.runner = self.context.cluster.node("runner")

        Feature(run=load("tests.test", "test"), flags=TE)


if main():
    regression()
