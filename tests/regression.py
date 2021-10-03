from testflows.core import *

from helpers.cluster import Cluster
from helpers.argparser import argparser

xfails = {
        # test_operator.py
        "/regression/e2e.test_operator/test_003*": [(Error, "Hits tf timeout")],
        "/regression/e2e.test_operator/test_006*": [(Error, "May hit timeout")],
        "/regression/e2e.test_operator/test_009. Test operator upgrade": [(Fail, "May fail due to label changes")],
        "/regression/e2e.test_operator/test_022. Test that chi with broken image can be deleted": [(Error, "Not supported yet. Timeout")],
        "/regression/e2e.test_operator/test_024. Test annotations for various template types/PV annotations should be populated": [(Fail, "Not supported yet")],

        # test_clickhouse.py
        "/regression/e2e.test_clickhouse/test_ch_001*": [(Fail, "Insert Quorum test need to refactoring")],

}


@TestSuite
@XFails(xfails)
@ArgumentParser(argparser)
def regression(self, native):
    """ClickHouse Operator test regression suite.
    """
    def run_features():
        features = [
            "e2e.test_operator",
            "e2e.test_clickhouse",
            "e2e.test_examples",
            "e2e.test_metrics_alerts",
            "e2e.test_metrics_exporter",
            "e2e.test_zookeeper",
            "e2e.test_backup_alerts",
        ]
        for feature_name in features:
            Feature(run=load(feature_name, "test"))

    self.context.native = native
    if native:
        run_features()
    else:
        with Cluster():
            run_features()


if main():
    regression()
