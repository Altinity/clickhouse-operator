#!/usr/bin/env python3
from testflows.core import *

from helpers.argparser import argparser
from helpers.cluster import Cluster
from requirements.requirements import *

xfails = {
    # test_operator.py
    "/regression/e2e.test_operator/test_010021*": [(Fail, "Storage test is flaky on github")],
    "/regression/e2e.test_operator/test_010082_1*": [(Fail, "Canary via CHIT injection does not work")],
    # NOTE: the blanket test_020005* xfail ("Keeper scale-up/scale-down is flaky")
    # is intentionally dropped on this branch. test_020005_2 must surface its
    # split-brain assertion as a real, honest FAIL on the pre-fix operator -- an
    # xfail would swallow exactly the red this branch exists to demonstrate.
}


@TestSuite
@XFails(xfails)
@ArgumentParser(argparser)
@Specifications(QA_SRS026_ClickHouse_Operator)
def regression(self, native, keeper_type, fips140_mode):
    """ClickHouse Operator test regression suite."""

    def run_features():
        features = [
            "e2e.test_metrics_exporter",
            "e2e.test_operator",
        ]
        for feature_name in features:
            Feature(run=load(feature_name, "test"))

    self.context.native = native
    self.context.keeper_type = keeper_type
    self.context.fips140_mode = "only"

    if native:
        run_features()
    else:
        with Cluster():
            run_features()


if main():
    regression()
