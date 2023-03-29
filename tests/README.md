# ClickHouse Operator tests

This folder contains TestFlows tests for ClickHouse Operator. This file describes how to launch those tests.

## Requirements

To execute tests, you will need:

* Python 3.8 or higher
* TestFlows Python library (`pip3 install -r ./tests/image/requirements.txt`)
* To run tests in docker container (approximately 2 times slower, but does not require any additional configuration):
    - `docker` 
    - `docker-compose`
    - `python3`
* To run tests natively on your machine:
    - `kubectl`
    - `python3`
    - `jq`
## Build test image

In order to run tests in docker, you will need a base image. By default, it will be pulled from GitLab registry.

You may also build the image locally. To do it, make the following steps:

```bash
docker login registry.gitlab.com
bash -xe ./tests/image/build_docker.sh
docker push registry.gitlab.com/altinity-public/container-images/clickhouse-operator-test-runner:latest
```

Be aware that building an image, as well as pulling it, will require about 5 GB of downloaded data.

## Execution

To execute the test suite (that currently involves only operator tests, not tests for third-party tools used by operator), execute the following command:

```bash
pip3 install -U -r ./tests/image/requirements.txt
docker pull registry.gitlab.com/altinity-public/container-images/clickhouse-operator-test-runner:latest
COMPOSE_HTTP_TIMEOUT=1800 python3 ./tests/regression.py --only "/regression/e2e.test_operator/*"
```

To execute tests natively (not in docker), you need to add `--native` parameter

To execute it in parallel mode, you need to add `--tests_in_parallel` parameter

If you need only one certain test, you may execute

```bash
COMPOSE_HTTP_TIMEOUT=1800 python3 ./tests/regression.py --only "/regression/e2e.test_operator/test_009*"
```

where `009` may be substituted by the number of the test you need. Tests --- numbers and names correspondence may be found in `tests/regression.py` and `tests/test_*.py` source code files.
