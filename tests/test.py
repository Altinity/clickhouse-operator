# main test runner
import kubectl
import settings
import test_clickhouse
import test_operator
import util

from testflows.core import Given, main, Scenario, Module, Fail, Error

if main():
    with Module("main"):
        with Given(f"Clean namespace {settings.test_namespace}"):
            kubectl.delete_all_chi(settings.test_namespace)
            kubectl.delete_ns(settings.test_namespace, ok_to_fail=True)
            kubectl.create_ns(settings.test_namespace)

        with Given(f"clickhouse-operator version {settings.operator_version} is installed"):
            if kubectl.get_count("pod", ns=settings.operator_namespace, label="-l app=clickhouse-operator") == 0:
                config = util.get_full_path(settings.clickhouse_operator_install)
                kubectl.apply(
                    ns=settings.operator_namespace,
                    config=f"<(cat {config} | "
                    f"OPERATOR_IMAGE=\"{settings.operator_docker_repo}:{settings.operator_version}\" "
                    f"OPERATOR_NAMESPACE=\"{settings.operator_namespace}\" "
                    f"METRICS_EXPORTER_IMAGE=\"{settings.metrics_exporter_docker_repo}:{settings.operator_version}\" "
                    f"METRICS_EXPORTER_NAMESPACE=\"{settings.operator_namespace}\" "
                    f"envsubst)",
                    validate=False
                )
            util.set_operator_version(settings.operator_version)

        with Given(f"Install ClickHouse template {settings.clickhouse_template}"):
            kubectl.apply(util.get_full_path(settings.clickhouse_template), settings.test_namespace)

        with Given(f"ClickHouse version {settings.clickhouse_version}"):
            pass

        # python3 tests/test.py --only operator*
        xfails = {
             "/main/operator/test_009. Test operator upgrade": [(Fail, "May fail due to label changes")],
             "/main/operator/test_022. Test that chi with broken image can be deleted": [(Error, "Not supported yet. Timeout")],
             "/main/operator/test_024. Test annotations for various template types/PV annotations should be populated": [(Fail, "Not supported yet")],
        }
        with Module("operator", xfails=xfails):
            all_tests = [
                test_operator.test_001,
                test_operator.test_002,
                test_operator.test_003,
                test_operator.test_004,
                test_operator.test_005,
                test_operator.test_006,
                test_operator.test_007,
                test_operator.test_008,
                (test_operator.test_009, {"version_from": "0.15.0"}),
                test_operator.test_010,
                test_operator.test_011,
                test_operator.test_011_1,
                test_operator.test_012,
                test_operator.test_013,
                test_operator.test_014,
                test_operator.test_015,
                test_operator.test_016,
                test_operator.test_017,
                test_operator.test_018,
                test_operator.test_019,
                test_operator.test_020,
                test_operator.test_021,
                test_operator.test_022,
                test_operator.test_023,
                test_operator.test_024,
                test_operator.test_025,
                test_operator.test_026,
                test_operator.test_027,
                test_operator.test_028,
                test_operator.test_029,
            ]
            run_tests = all_tests

            # placeholder for selective test running
            # run_tests = [test_008, (test_009, {"version_from": "0.9.10"})]
            # run_tests = [test_002]

            for t in run_tests:
                if callable(t):
                    Scenario(test=t)()
                else:
                    Scenario(test=t[0], args=t[1])()

        # python3 tests/test.py --only clickhouse*
        with Module("clickhouse"):
            all_tests = [
                test_clickhouse.test_ch_001,
                test_clickhouse.test_ch_002,
            ]

            run_test = all_tests

            # placeholder for selective test running
            # run_test = [test_ch_002]

            for t in run_test:
                Scenario(test=t)()
