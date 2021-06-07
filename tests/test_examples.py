from testflows.core import TestScenario, Name, When, Then, Given, And, main, Scenario, Module
from testflows.asserts import error

import kubectl


@TestScenario
@Name("Empty installation, creates 1 node")
def test_examples01_1():
    kubectl.create_and_check(
        config="../docs/chi-examples/01-simple-layout-01-1shard-1repl.yaml",
        check={
            "object_counts": {
                "statefulset": 1,
                "pod": 1,
                "service": 2,
            }
        })


@TestScenario
@Name("1 shard 2 replicas")
def test_examples01_2():
    kubectl.create_and_check(
        config="../docs/chi-examples/01-simple-layout-02-1shard-2repl.yaml",
        check={
            "object_counts": {
                "statefulset": 2,
                "pod": 2,
                "service": 3,
            }
        })


@TestScenario
@Name("Persistent volume mapping via defaults")
def test_examples02_1():
    kubectl.create_and_check(
        config="../docs/chi-examples/03-persistent-volume-01-default-volume.yaml",
        check={
            "pod_count": 1,
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/log/clickhouse-server",
            },
        })


@TestScenario
@Name("Persistent volume mapping via podTemplate")
def test_examples02_2():
    kubectl.create_and_check(
        config="../docs/chi-examples/03-persistent-volume-02-pod-template.yaml",
        check={
            "pod_count": 1,
            "pod_image": "yandex/clickhouse-server:19.3.7",
            "pod_volumes": {
                "/var/lib/clickhouse",
                "/var/log/clickhouse-server",
            },
        })


if main():
    with Module("examples"):
        examples = [test_examples01_1, test_examples01_2, test_examples02_1, test_examples02_2]
        for t in examples:
            Scenario(test=t)
