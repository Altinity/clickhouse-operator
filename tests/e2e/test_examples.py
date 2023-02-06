from testflows.core import *
import e2e.kubectl as kubectl
import e2e.util as util


@TestScenario
@Name("test_examples01_1. Empty installation, creates 1 node")
def test_examples01_1(self):
    kubectl.create_and_check(
        manifest="../../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml",
        check={
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            }
        },
    )


@TestScenario
@Name("test_examples01_2. 1 shard 2 replicas")
def test_examples01_2(self):
    kubectl.create_and_check(
        manifest="../../docs/chi-examples/01-simple-layout-02-1shard-2repl.yaml",
        check={
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 3,
            }
        },
    )


@TestScenario
@Name("test_examples02_1. Persistent volume mapping via defaults")
def test_examples02_1(self):
    kubectl.create_and_check(
        manifest="../../docs/chi-examples/03-persistent-volume-01-default-volume.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/log/clickhouse-server",
            },
        },
    )


@TestScenario
@Name("test_examples02_2. Persistent volume mapping via podTemplate")
def test_examples02_2(self):
    kubectl.create_and_check(
        manifest="../../docs/chi-examples/03-persistent-volume-02-pod-template.yaml",
        check={
            "pod_count": 1,
            "pod_image": "clickhouse/clickhouse-server:22.3",
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/log/clickhouse-server",
            },
        },
    )


@TestFeature
@Name("e2e.test_examples")
def test(self):
    util.clean_namespace(delete_chi=False)
    examples = [
        test_examples01_1,
        test_examples01_2,
        test_examples02_1,
        test_examples02_2,
    ]
    for t in examples:
        Scenario(test=t)()
