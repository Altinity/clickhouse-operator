from testflows.core import *

from helpers.cluster import Cluster
from helpers.argparser import argparser
import e2e.test as test

xfails = {
        "/clickhouse operator/test/main module/operator/test_003*": [(Error, "Hits tf timeout")],
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
    with Cluster():
        Feature(run=load("e2e.test", "test"), flags=TE)


if main():
    regression()
