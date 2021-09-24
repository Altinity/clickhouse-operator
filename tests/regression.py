from testflows.core import *

from helpers.cluster import Cluster
from helpers.argparser import argparser

xfails = {
        "/clickhouse_operator/test/main/operator/test_003*": [(Error, "Hits tf timeout")],
        "/clickhouse_operator/test/main/operator/test_006*": [(Error, "May hit timeout")],
        "/clickhouse_operator/test/main/operator/test_009. Test operator upgrade": [(Fail, "May fail due to label changes")],
        "/clickhouse_operator/test/main/operator/test_022. Test that chi with broken image can be deleted": [(Error, "Not supported yet. Timeout")],
        "/clickhouse_operator/test/main/operator/test_024. Test annotations for various template types/PV annotations should be populated": [(Fail, "Not supported yet")],
    }


@TestSuite
@Name("clickhouse_operator")
@XFails(xfails)
@ArgumentParser(argparser)
def regression(self, clickhouse_image, operator_version, native):
    """ClickHouse Operator test regression suite.
    """
    self.context.native = native
    if native:
        Feature(run=load("e2e.test", "test"))
    else:
        with Cluster():
            Feature(run=load("e2e.test", "test"))


if main():
    regression()
