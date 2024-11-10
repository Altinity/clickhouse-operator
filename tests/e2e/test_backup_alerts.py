import os
os.environ["TEST_NAMESPACE"]="test-backup-alerts"

import json
import random
import time
import datetime
import e2e.alerts as alerts
import e2e.settings as settings
import e2e.kubectl as kubectl
import e2e.clickhouse as clickhouse
import e2e.util as util
import e2e.steps as steps

from testflows.core import *
from testflows.asserts import error


def get_minio_spec():
    with Given("get information about prometheus installation"):
        minio_spec = kubectl.get("pod", ns=settings.minio_namespace, name="", label="-l app=minio")
        assert "items" in minio_spec and len(minio_spec["items"]) and "metadata" in minio_spec["items"][0], error(
            "invalid minio spec, please run install-minio.sh"
        )
        return minio_spec


def exec_on_backup_container(
    backup_pod,
    cmd,
    ns,
    ok_to_fail=False,
    timeout=60,
    container="clickhouse-backup",
):
    return kubectl.launch(
        f"exec -n {ns} {backup_pod} -c {container} -- {cmd}",
        ok_to_fail=ok_to_fail,
        timeout=timeout,
    )


def get_backup_metric_value(backup_pod, metric_name, ns):
    cmd = (
        f"curl -sL http://127.0.0.1:7171/metrics | grep -E '^({metric_name}) [+-]?[0-9]+([.][0-9]+)?' | cut -d ' ' -f 2"
    )
    return exec_on_backup_container(backup_pod, cmd=cmd, ns=ns)


def is_expected_backup_status(command_name, command_is_done, st, expected_status, err_status):
    if "command" in st and st["command"] == command_name:
        if st["status"] == expected_status:
            command_is_done = True
            return True, command_is_done
        elif st["status"] == err_status:
            if "error" in st:
                fail(st["error"])
            else:
                fail(f"unexpected status of {command_name} {st}")
        else:
            with Then("Not ready, wait 5 sec"):
                time.sleep(5)
    return False, command_is_done


def wait_backup_command_status(backup_pod, command_name, ns, expected_status="success", err_status="error"):
    command_is_done = False
    with Then(f'wait "{command_name}" with status "{expected_status}"'):
        while command_is_done is False:
            status_lines = exec_on_backup_container(
                backup_pod, f'curl -sL "http://127.0.0.1:7171/backup/status"',
                ns=ns
            ).splitlines()
            for line in status_lines:
                st = json.loads(line)
                is_break, command_is_done = is_expected_backup_status(
                    command_name, command_is_done, st, expected_status, err_status
                )
                if is_break:
                    break


def wait_backup_pod_ready_and_curl_installed(backup_pod):
    with Then(f"wait {backup_pod} ready"):
        kubectl.wait_field("pod", backup_pod, ".status.containerStatuses[1].ready", "true")
        kubectl.launch(f"exec {backup_pod} -c clickhouse-backup -- curl --version")


def prepare_table_for_backup(backup_pod, chi, rows=1000):
    backup_name = f'test_backup_{time.strftime("%Y-%m-%d_%H%M%S")}'
    clickhouse.query(
        chi["metadata"]["name"],
        "CREATE TABLE IF NOT EXISTS default.test_backup ON CLUSTER 'all-sharded' (i UInt64) ENGINE MergeTree() ORDER BY tuple();"
        f"INSERT INTO default.test_backup SELECT number FROM numbers({rows})",
        pod=backup_pod,
    )
    return backup_name


def apply_fake_backup(message):
    with Given(message):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-backups-fake.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-backups-fake.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )


def apply_normal_backup():
    with Then("apply back normal backup"):
        kubectl.create_and_check(
            manifest="manifests/chi/test-cluster-for-backups.yaml",
            check={
                "apply_templates": [
                    "manifests/chit/tpl-clickhouse-backups.yaml",
                    "manifests/chit/tpl-persistent-volume-100Mi.yaml",
                ],
                "object_counts": {
                    "statefulset": 2,
                    "pod": 2,
                    "service": 3,
                },
                "do_not_delete": 1,
            },
        )


@TestScenario
@Name("Check clickhouse-operator/minio setup")
def test_minio_setup(self, chi, minio_spec):
    with Given("clickhouse-operator is installed"):
        assert (
            kubectl.get_count(
                "pod",
                ns=settings.operator_namespace,
                label="-l app=clickhouse-operator",
            )
            > 0
        ), error("please run deploy/operator/clickhouse-operator-install.sh before run test")
        util.set_operator_version(settings.operator_version)
        util.set_metrics_exporter_version(settings.operator_version)

    with Given("minio-operator is installed"):
        assert kubectl.get_count("pod", ns=settings.minio_namespace, label="-l name=minio-operator") > 0, error(
            "please run deploy/minio/install-minio-operator.sh before test run"
        )
        assert kubectl.get_count("pod", ns=settings.minio_namespace, label="-l app=minio") > 0, error(
            "please run deploy/minio/install-minio.sh before test run"
        )

        minio_expected_version = f"minio/minio:{settings.minio_version}"
        assert minio_expected_version in minio_spec["items"][0]["spec"]["containers"][0]["image"], error(
            f"require {minio_expected_version} image"
        )


@TestScenario
@Name("test_backup_is_success# Basic backup scenario")
def test_backup_is_success(self, chi, minio_spec):
    _, _, backup_pod, _ = alerts.random_pod_choice_for_callbacks(chi)
    backup_name = prepare_table_for_backup(backup_pod, chi)
    wait_backup_pod_ready_and_curl_installed(backup_pod)

    with When("Backup is success"):
        backup_successful_before = get_backup_metric_value(
            backup_pod,
            "clickhouse_backup_successful_backups|clickhouse_backup_successful_creates",
            ns=self.context.test_namespace
        )
        list_before = exec_on_backup_container(backup_pod, "curl -sL http://127.0.0.1:7171/backup/list", self.context.test_namespace)
        exec_on_backup_container(
            backup_pod,
            f'curl -X POST -sL "http://127.0.0.1:7171/backup/create?name={backup_name}"',
            ns=self.context.test_namespace
        )
        wait_backup_command_status(backup_pod, f"create {backup_name}", expected_status="success", ns=self.context.test_namespace,)

        exec_on_backup_container(
            backup_pod,
            f'curl -X POST -sL "http://127.0.0.1:7171/backup/upload/{backup_name}"',
            ns=self.context.test_namespace
        )
        wait_backup_command_status(backup_pod, f"upload {backup_name}", expected_status="success", ns=self.context.test_namespace)

    with Then("list of backups shall changed"):
        list_after = exec_on_backup_container(
            backup_pod, "curl -sL http://127.0.0.1:7171/backup/list",
            ns=self.context.test_namespace
        )
        assert list_before != list_after, error("backup is not created")

    with Then("successful backup count shall increased"):
        backup_successful_after = get_backup_metric_value(
            backup_pod,
            "clickhouse_backup_successful_backups|clickhouse_backup_successful_creates",
            ns=self.context.test_namespace,
        )
        assert backup_successful_before != backup_successful_after, error(
            "clickhouse_backup_successful_backups shall increased"
        )


@TestScenario
@Name("test_backup_is_down# ClickHouseBackupDown and ClickHouseBackupRecentlyRestart alerts")
def test_backup_is_down(self, chi, minio_spec):
    reboot_pod, _, _, _ = alerts.random_pod_choice_for_callbacks(chi)

    def reboot_backup_container():
        kubectl.launch(
            f"exec -n {self.context.test_namespace} {reboot_pod} -c clickhouse-backup -- kill 1",
            ok_to_fail=True,
        )

    with When("reboot clickhouse-backup"):
        fired = alerts.wait_alert_state(
            "ClickHouseBackupDown",
            "firing",
            expected_state=True,
            callback=reboot_backup_container,
            labels={"pod_name": reboot_pod},
            time_range="60s",
        )
        assert fired, error("can't get ClickHouseBackupDown alert in firing state")

    with Then("check ClickHouseBackupDown gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseBackupDown",
            "firing",
            expected_state=False,
            sleep_time=settings.prometheus_scrape_interval,
            labels={"pod_name": reboot_pod},
        )
        assert resolved, error("can't get ClickHouseBackupDown alert is gone away")

    with When("reboot clickhouse-backup again"):
        fired = alerts.wait_alert_state(
            "ClickHouseBackupRecentlyRestart",
            "firing",
            expected_state=True,
            callback=reboot_backup_container,
            labels={"pod_name": reboot_pod},
            time_range="60s",
        )
        assert fired, error("can't get ClickHouseBackupRecentlyRestart alert in firing state")

    with Then("check ClickHouseBackupRecentlyRestart gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseBackupRecentlyRestart",
            "firing",
            expected_state=False,
            time_range="30s",
            labels={"pod_name": reboot_pod},
            sleep_time=30,
        )
        assert resolved, error("can't get ClickHouseBackupRecentlyRestart alert is gone away")


@TestScenario
@Name("test_backup_failed# Check ClickHouseBackupFailed alerts")
def test_backup_failed(self, chi, minio_spec):
    backup_pod, _, _, _ = alerts.random_pod_choice_for_callbacks(chi)
    backup_prefix = prepare_table_for_backup(backup_pod, chi)

    wait_backup_pod_ready_and_curl_installed(backup_pod)

    def create_fail_backup():
        backup_name = backup_prefix + "-" + str(random.randint(1, 4096))
        backup_dir = f"/var/lib/clickhouse/backup/{backup_name}/shadow/default/test_backup"
        kubectl.launch(
            f"exec -n {self.context.test_namespace} {backup_pod} -c clickhouse-backup -- bash -c 'mkdir -v -m 0400 -p {backup_dir}'",
        )

        kubectl.launch(
            f"exec -n {self.context.test_namespace} {backup_pod} -c clickhouse-backup -- curl -X POST -sL http://127.0.0.1:7171/backup/create?name={backup_name}",
        )
        wait_backup_command_status(backup_pod, command_name=f"create {backup_name}", expected_status="error", ns=self.context.test_namespace)

    def create_success_backup():
        backup_name = backup_prefix + "-" + str(random.randint(1, 4096))
        kubectl.launch(
            f"exec -n {self.context.test_namespace} {backup_pod} -c clickhouse-backup -- curl -X POST -sL http://127.0.0.1:7171/backup/create?name={backup_name}",
        )
        wait_backup_command_status(backup_pod, command_name=f"create {backup_name}", expected_status="success", ns=self.context.test_namespace)

    with When("clickhouse-backup create failed"):
        fired = alerts.wait_alert_state(
            "ClickHouseBackupFailed",
            "firing",
            expected_state=True,
            callback=create_fail_backup,
            labels={"pod_name": backup_pod},
            time_range="60s",
        )
        assert fired, error("can't get ClickHouseBackupFailed alert in firing state")

    with Then("check ClickHouseBackupFailed gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseBackupFailed",
            "firing",
            expected_state=False,
            callback=create_success_backup,
            labels={"pod_name": backup_pod},
            sleep_time=30,
        )
        assert resolved, error("can't get ClickHouseBackupFailed alert is gone away")


@TestScenario
@Name("test_backup_duration# Check ClickHouseBackupTooShort and ClickHouseBackupTooLong alerts")
def test_backup_duration(self, chi, minio_spec):
    short_pod, _, long_pod, _ = alerts.random_pod_choice_for_callbacks(chi)
    apply_fake_backup("prepare fake backup duration metric")

    for pod in [short_pod, long_pod]:
        with Then(f"wait {pod} ready"):
            kubectl.wait_field("pod", pod, ".spec.containers[1].image", "nginx:latest")
            kubectl.wait_field("pod", pod, ".status.containerStatuses[1].ready", "true")

            fired = alerts.wait_alert_state(
                "ClickHouseBackupTooLong",
                "firing",
                expected_state=True,
                sleep_time=settings.prometheus_scrape_interval,
                labels={"pod_name": pod},
                time_range="60s",
            )
            assert fired, error(f"can't get ClickHouseBackupTooLong alert in firing state for {pod}")

    with Then(f"wait when prometheus will scrape fake data"):
        time.sleep(70)

    with Then(f"decrease {short_pod} backup duration"):
        kubectl.launch(
            f"exec {short_pod} -c clickhouse-backup -- bash -xc '"
            'echo "# HELP clickhouse_backup_last_create_duration Backup create duration in nanoseconds" > /usr/share/nginx/html/metrics && '
            'echo "# TYPE clickhouse_backup_last_create_duration gauge" >> /usr/share/nginx/html/metrics && '
            'echo "clickhouse_backup_last_create_duration 7000000000000" >> /usr/share/nginx/html/metrics && '
            'echo "# HELP clickhouse_backup_last_create_status Last backup create status: 0=failed, 1=success, 2=unknown" >> /usr/share/nginx/html/metrics && '
            'echo "# TYPE clickhouse_backup_last_create_status gauge" >> /usr/share/nginx/html/metrics && '
            'echo "clickhouse_backup_last_create_status 1" >> /usr/share/nginx/html/metrics'
            "'"
        )

        fired = alerts.wait_alert_state(
            "ClickHouseBackupTooShort",
            "firing",
            expected_state=True,
            sleep_time=settings.prometheus_scrape_interval,
            labels={"pod_name": short_pod},
            time_range="60s",
        )
        assert fired, error("can't get ClickHouseBackupTooShort alert in firing state")

    apply_normal_backup()

    with Then("check ClickHouseBackupTooShort gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseBackupTooShort",
            "firing",
            expected_state=False,
            labels={"pod_name": short_pod},
        )
        assert resolved, error("can't get ClickHouseBackupTooShort alert is gone away")

    with Then("check ClickHouseBackupTooLong gone away"):
        resolved = alerts.wait_alert_state(
            "ClickHouseBackupTooLong",
            "firing",
            expected_state=False,
            labels={"pod_name": long_pod},
        )
        assert resolved, error("can't get ClickHouseBackupTooLong alert is gone away")


@TestScenario
@Name("test_backup_size# Check ClickHouseBackupSizeChanged alerts")
def test_backup_size(self, chi, minio_spec):
    decrease_pod, _, increase_pod, _ = alerts.random_pod_choice_for_callbacks(chi)

    backup_cases = {
        decrease_pod: {
            "rows": (10000, 1000),
            "decrease": True,
        },
        increase_pod: {
            "rows": (1000, 10000),
            "decrease": False,
        },
    }
    for backup_pod in backup_cases:
        decrease = backup_cases[backup_pod]["decrease"]
        for backup_rows in backup_cases[backup_pod]["rows"]:
            backup_name = prepare_table_for_backup(backup_pod, chi, rows=backup_rows)
            exec_on_backup_container(
                backup_pod,
                f'curl -X POST -sL "http://127.0.0.1:7171/backup/create?name={backup_name}"',
                ns=self.context.test_namespace
            )
            wait_backup_command_status(backup_pod, f"create {backup_name}", expected_status="success", ns=self.context.test_namespace)
            if decrease:
                clickhouse.query(
                    chi["metadata"]["name"],
                    f"TRUNCATE TABLE default.test_backup",
                    pod=backup_pod,
                )
            time.sleep(15)
        fired = alerts.wait_alert_state(
            "ClickHouseBackupSizeChanged",
            "firing",
            expected_state=True,
            sleep_time=settings.prometheus_scrape_interval,
            labels={"pod_name": backup_pod},
            time_range="60s",
        )
        assert fired, error(f"can't get ClickHouseBackupSizeChanged alert in firing state, decrease={decrease}")

        with Then("check ClickHouseBackupSizeChanged gone away"):
            resolved = alerts.wait_alert_state(
                "ClickHouseBackupSizeChanged",
                "firing",
                expected_state=False,
                labels={"pod_name": backup_pod},
            )
            assert resolved, error(f"can't get ClickHouseBackupSizeChanged alert is gone away, decrease={decrease}")


@TestScenario
@Name("test_backup_not_run# Check ClickhouseBackupDoesntRunTooLong alert")
def test_backup_not_run(self, chi, minio_spec):
    not_run_pod, _, _, _ = alerts.random_pod_choice_for_callbacks(chi)
    apply_fake_backup("prepare fake backup for time metric")

    with Then(f"wait {not_run_pod} ready"):
        kubectl.wait_field("pod", not_run_pod, ".spec.containers[1].image", "nginx:latest")
        kubectl.wait_field("pod", not_run_pod, ".status.containerStatuses[1].ready", "true")

    with Then(f"setup {not_run_pod} backup create end time"):
        kubectl.launch(
            f"exec {not_run_pod} -c clickhouse-backup -- bash -xc '"
            'echo "# HELP clickhouse_backup_last_create_finish Last backup create finish timestamp" > /usr/share/nginx/html/metrics && '
            'echo "# TYPE clickhouse_backup_last_create_finish gauge" >> /usr/share/nginx/html/metrics && '
            f'echo "clickhouse_backup_last_create_finish {int((datetime.datetime.now() - datetime.timedelta(days=2)).timestamp())}" >> /usr/share/nginx/html/metrics '
            "'"
        )

        fired = alerts.wait_alert_state(
            "ClickhouseBackupDoesntRunTooLong",
            "firing",
            expected_state=True,
            sleep_time=settings.prometheus_scrape_interval,
            labels={"pod_name": not_run_pod},
            time_range="60s",
        )
        assert fired, error("can't get ClickhouseBackupDoesntRunTooLong alert in firing state")

    apply_normal_backup()

    backup_name = prepare_table_for_backup(not_run_pod, chi)
    wait_backup_pod_ready_and_curl_installed(not_run_pod)

    with When("Backup is success"):
        exec_on_backup_container(
            not_run_pod,
            f'curl -X POST -sL "http://127.0.0.1:7171/backup/create?name={backup_name}"',
            ns=self.context.test_namespace
        )
        wait_backup_command_status(not_run_pod, f"create {backup_name}", expected_status="success", ns=self.context.test_namespace)

        exec_on_backup_container(
            not_run_pod,
            f'curl -X POST -sL "http://127.0.0.1:7171/backup/upload/{backup_name}"',
            ns=self.context.test_namespace
        )
        wait_backup_command_status(not_run_pod, f"upload {backup_name}", expected_status="success", ns=self.context.test_namespace)

    with Then("check ClickhouseBackupDoesntRunTooLong gone away"):
        resolved = alerts.wait_alert_state(
            "ClickhouseBackupDoesntRunTooLong",
            "firing",
            expected_state=False,
            labels={"pod_name": not_run_pod},
        )
        assert resolved, error("can't get ClickhouseBackupDoesntRunTooLong alert is gone away")


@TestModule
@Name("e2e.test_backup_alerts")
def test(self):
    with Given("I setup settings"):
        steps.set_settings()
    with Given("I create shell"):
        shell = steps.get_shell()
        self.context.shell = shell

    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()

    _, _, _, _, chi = alerts.initialize(
        chi_file="manifests/chi/test-cluster-for-backups.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-backups.yaml",
        chi_name="test-cluster-for-backups",
        keeper_type=self.context.keeper_type,
    )
    minio_spec = get_minio_spec()

    with Module("backup_alerts"):
        all_tests = [
            test_backup_is_success,
            test_backup_is_down,
            test_backup_failed,
            test_backup_duration,
            test_backup_size,
            test_backup_not_run,
        ]
        for t in all_tests:
            Scenario(test=t)(chi=chi, minio_spec=minio_spec)

    util.clean_namespace(delete_chi=True, delete_keeper=True, namespace=self.context.test_namespace)
