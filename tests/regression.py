import sys
from testflows.core import *

from helpers.cluster import Cluster
from helpers.argparser import argparser

xfails = {
        "/clickhouse operator/test/main module/operator/test_006*": [(Error, "May hit timeout")],
        "/clickhouse operator/test/main module/operator/test_009. Test operator upgrade": [(Fail, "May fail due to label changes")],
        "/clickhouse operator/test/main module/operator/test_022. Test that chi with broken image can be deleted": [(Error, "Not supported yet. Timeout")],
        "/clickhouse operator/test/main module/operator/test_024. Test annotations for various template types/PV annotations should be populated": [(Fail, "Not supported yet")],
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
