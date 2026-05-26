#!/usr/bin/env python3
from testflows.core import *

from helpers.argparser import argparser
from helpers.cluster import Cluster
from requirements.requirements import *

xfails = {
    # test_operator.py
    "/regression/e2e.test_operator/test_010021*": [(Fail, "Storage test is flaky on github")],
    "/regression/e2e.test_operator/test_020005*": [(Fail, "Keeper scale-up/scale-down is flaky")],
    # test_clickhouse.py
    "/regression/e2e.test_clickhouse/test_ch_001*": [(Fail, "Insert Quorum test need to refactoring")],
}


@TestSuite
@XFails(xfails)
@ArgumentParser(argparser)
@Specifications(QA_SRS026_ClickHouse_Operator)
def regression(self, native, keeper_type):
    """ClickHouse Operator test regression suite."""

    def run_features():
        features = [
            "e2e.test_metrics_exporter",
            "e2e.test_metrics_alerts",
            "e2e.test_backup_alerts",
            "e2e.test_operator",
            "e2e.test_clickhouse",
            "e2e.test_examples",
            "e2e.test_acvp",
        ]
        for feature_name in features:
            Feature(run=load(feature_name, "test"))

    self.context.native = native
    self.context.keeper_type = keeper_type

    if native:
        run_features()
    else:
        with Cluster():
            run_features()


if main():
    regression()
