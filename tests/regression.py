#!/usr/bin/env python3
from testflows.core import *

from helpers.argparser import argparser
from helpers.cluster import Cluster
from requirements.requirements import *

xfails = {
    # test_operator.py
    #"/regression/e2e.test_operator/test_009*": [(Fail, "deal with it later")],
    # "/regression/e2e.test_operator/test_014_0*": [(Fail, "Hidden properties fail prior 0.25.x")],
    "/regression/e2e.test_operator/test_049*": [(Fail, "Keeper is flaky")],
    "/regression/e2e.test_operator/test_052*": [(Fail, "Keeper scale-up/scale-down is flaky")],
    # test_clickhouse.py
    "/regression/e2e.test_clickhouse/test_ch_001*": [(Fail, "Insert Quorum test need to refactoring")],
    # test_metrics_alerts.py
    # "/regression/e2e.test_metrics_alerts/test_clickhouse_keeper_alerts*": [
    #     (Fail, "clickhouse-keeper wrong prometheus endpoint format, look https://github.com/ClickHouse/ClickHouse/issues/46136")
    # ],
    # test_keeper.py
    # "/regression/e2e.test_keeper/test_clickhouse_keeper_rescale*": [
    #     (Fail, "need `ruok` before quorum https://github.com/ClickHouse/ClickHouse/issues/35464, need apply file config instead use commited data for quorum https://github.com/ClickHouse/ClickHouse/issues/35465. --force-recovery useless https://github.com/ClickHouse/ClickHouse/issues/37434"),
    # ],
    # "/regression/e2e.test_metrics_alerts/test_clickhouse_dns_errors*": [
    #     (Fail, "DNSError behavior changed on 21.9, look https://github.com/ClickHouse/ClickHouse/issues/29624")
    # ],

    # test_keeper.py
    "/regression/e2e.test_keeper/test_zookeeper_operator_probes_workload*": [
        (
            Fail,
            "zookeeper liveness probe doesn't work, wait when https://github.com/pravega/zookeeper-operator/pull/476 will merge",
        )
    ],
    # "/regression/e2e.test_keeper/test_clickhouse_keeper_probes_workload*": [
    #     (Fail, "clickhouse-keeper fail after insert 10000 parts, look https://github.com/ClickHouse/ClickHouse/issues/35712")
    # ],
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
            "e2e.test_keeper",
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
