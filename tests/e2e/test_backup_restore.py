import time

import e2e.settings as settings
import e2e.kubectl as kubectl
import e2e.clickhouse as clickhouse
import e2e.util as util
import e2e.steps as steps
import e2e.alerts as alerts

from testflows.core import *
from testflows.asserts import error

CHI_NAME = "test-cluster-for-backups"
REPLICA_0 = "chi-test-cluster-for-backups-default-0-0"
REPLICA_1 = "chi-test-cluster-for-backups-default-0-1"
ROWS = 1000


def wait_backup_sidecars_ready():
    for pod in (REPLICA_0, REPLICA_1):
        with Then(f"wait {pod} clickhouse-backup sidecar ready"):
            kubectl.wait_field("pod", pod, ".status.containerStatuses[1].ready", "true")


def wait_cr_phase(kind, name, expected="Completed", timeout=300):
    """Poll a backup/restore custom resource until it reaches the expected phase."""
    with Then(f'wait {kind}/{name} phase "{expected}"'):
        start = time.time()
        while time.time() - start < timeout:
            phase = kubectl.launch(
                f"get {kind} {name} -n {settings.test_namespace} -o jsonpath='{{.status.phase}}'",
                ok_to_fail=True,
            ).strip("'")
            if phase == expected:
                return True
            if phase == "Failed":
                fail(f"{kind}/{name} reached Failed phase")
            time.sleep(5)
    fail(f"{kind}/{name} did not reach phase {expected} within {timeout}s")


def create_replicated_table_with_data():
    with Given("a ReplicatedMergeTree table with data on the cluster"):
        clickhouse.query(
            CHI_NAME,
            "CREATE TABLE IF NOT EXISTS default.test_restore ON CLUSTER 'default' (i UInt64) "
            "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}') ORDER BY tuple();"
            f"INSERT INTO default.test_restore SELECT number FROM numbers({ROWS})",
            pod=REPLICA_0,
        )


def row_count(pod):
    return clickhouse.query(CHI_NAME, "SELECT count() FROM default.test_restore", pod=pod).strip()


@TestScenario
@Name("test_operator_backup_and_restore. Operator-driven backup then restore round-trip")
def test_operator_backup_and_restore(self, chi):
    wait_backup_sidecars_ready()
    create_replicated_table_with_data()

    with When("a ClickHouseBackup is created"):
        kubectl.launch(f"apply -f manifests/chb/test-clickhousebackup.yaml -n {settings.test_namespace}")
        wait_cr_phase("clickhousebackup", "test-backup", "Completed")

    with Then("the operator created an owned backup Job"):
        kubectl.wait_field("job", "test-backup-backup", ".status.succeeded", "1")

    with When("the table is dropped on the whole cluster"):
        clickhouse.query(CHI_NAME, "DROP TABLE default.test_restore ON CLUSTER 'default' SYNC", pod=REPLICA_0)

    with When("a ClickHouseRestore is created"):
        kubectl.launch(f"apply -f manifests/chr/test-clickhouserestore.yaml -n {settings.test_namespace}")
        wait_cr_phase("clickhouserestore", "test-restore", "Completed")

    with Then("data is restored on the first replica"):
        assert row_count(REPLICA_0) == str(ROWS), error("data not restored on first replica")

    with Then("native replication synchronized the second replica"):
        synced = False
        for _ in range(24):
            if row_count(REPLICA_1) == str(ROWS):
                synced = True
                break
            time.sleep(5)
        assert synced, error("second replica did not catch up after restore")


@TestScenario
@Name("test_backup_schedule_creates_cronjob. Schedule is reconciled into a CronJob")
def test_backup_schedule_creates_cronjob(self, chi):
    with When("a ClickHouseBackupSchedule is created"):
        kubectl.launch(f"apply -f manifests/chbs/test-clickhousebackupschedule.yaml -n {settings.test_namespace}")

    with Then("the operator reconciles a managed CronJob"):
        kubectl.wait_field(
            "clickhousebackupschedule", "test-backup-schedule", ".status.cronJobName", "test-backup-schedule-backup"
        )
        cronjob = kubectl.get("cronjob", "test-backup-schedule-backup", ns=settings.test_namespace)
        assert cronjob["spec"]["schedule"] == "0 2 * * *", error("unexpected cronjob schedule")
        assert cronjob["spec"]["concurrencyPolicy"] == "Forbid", error("unexpected concurrencyPolicy")

    with Finally("cleanup the schedule"):
        kubectl.launch(
            f"delete -f manifests/chbs/test-clickhousebackupschedule.yaml -n {settings.test_namespace}",
            ok_to_fail=True,
        )


@TestModule
@Name("e2e.test_backup_restore")
def test(self):
    with Given("I setup settings"):
        steps.set_settings()
    with Given("I create shell"):
        self.context.shell = steps.get_shell()

    util.clean_namespace(delete_chi=True)
    util.install_operator_if_not_exist()

    _, _, _, _, chi = alerts.initialize(
        chi_file="manifests/chi/test-cluster-for-backups.yaml",
        chi_template_file="manifests/chit/tpl-clickhouse-backups.yaml",
        chi_name=CHI_NAME,
        keeper_type=self.context.keeper_type,
    )

    with Module("backup_restore"):
        for scenario in (test_operator_backup_and_restore, test_backup_schedule_creates_cronjob):
            Scenario(test=scenario)(chi=chi)
