from clickhouse import * 
from kubectl import *
import settings 
from test_operator import *
from test_clickhouse import *  

from testflows.core import TestScenario, Name, When, Then, Given, And, main, run, Module, TE, args
from testflows.asserts import error

if main():
    with Module("main"):
        with Given(f"Clean namespace {settings.test_namespace}"):
            kube_delete_all_chi(settings.test_namespace)
            kube_deletens(settings.test_namespace)
            kube_createns(settings.test_namespace)

        with Given(f"clickhouse-operator version {settings.operator_version} is installed"):
            if kube_get_count("pod", ns=settings.operator_namespace, label="-l app=clickhouse-operator") == 0:
                config = get_full_path('../deploy/operator/clickhouse-operator-install-template.yaml')
                kube_apply(f"<(cat {config} | "
                           f"OPERATOR_IMAGE=\"altinity/clickhouse-operator:{settings.operator_version}\" "
                           f"OPERATOR_NAMESPACE=\"{settings.operator_namespace}\" "
                           f"METRICS_EXPORTER_IMAGE=\"altinity/metrics-exporter:{settings.operator_version}\" "
                           f"METRICS_EXPORTER_NAMESPACE=\"{settings.operator_namespace}\" "
                           f"envsubst)", ns=settings.operator_namespace, validate=False)
            set_operator_version(settings.operator_version)

        with Given(f"Install ClickHouse template {settings.clickhouse_template}"):
            kube_apply(get_full_path(settings.clickhouse_template), settings.test_namespace)

        with Given(f"ClickHouse version {settings.clickhouse_version}"):
            pass

        # python3 tests/test.py --only operator*
        with Module("operator"):
            all_tests = [
                test_001,
                test_002,
                test_004,
                test_005,
                test_006,
                test_007,
                test_008,
                (test_009, {"version_from": "0.9.10"}),
                test_010,
                test_011,
                test_011_1,
                test_012,
                test_013,
                test_014,
                test_015,
                test_016,
                test_017,
                test_018,
                test_019,
                test_020,
                test_021,
                test_022,
            ]
            run_tests = all_tests
            
            # placeholder for selective test running
            # run_tests = [test_008, (test_009, {"version_from": "0.9.10"})]
            # run_tests = [test_002]

            for t in run_tests:
                if callable(t):
                    run(test=t)
                else:
                    run(test=t[0], args=t[1])

        # python3 tests/test.py --only clickhouse*
        with Module("clickhouse"):
            all_tests = [
                test_ch_001,
                test_ch_002,
            ]
        
            run_test = all_tests
            
            # placeholder for selective test running
            # run_test = [test_ch_002]

            for t in run_test:
                run(test=t)
