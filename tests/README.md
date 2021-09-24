# ClickHouse Operator tests

This folder contains TestFlows tests for ClickHouse Operator. This file describes how to launch those tests.

## Requirements

To execute tests, you will need:

* Python 3.8 or higher
* TestFlows Python library (`pip3 install -r ./tests/image/requirements.txt`)
* To run tests in docker container (approximately 2 times slower, but does not require any additional configuration):
    - `docker` and `docker-compose`
* To run tests natively on your machine:
    - `kubectl`
    - `docker`

## Build test image

In order to run tests in docker, you will need a base image. By default, it will be pulled from GitLab registry.

You may also build the image locally. To do it, make the following steps:

```bash
docker login registry.gitlab.com
bash -xe ./tests/image/build_docker.sh
```

Be aware that building an image, as well as pulling it, will require about 5 GB of downloaded data.

## Execution

To execute the test suite (that currently involves only operator tests, not tests for third-party tools used by operator), execute the following command:

```bash
python3 regression.py --only "/clickhouse_operator/test/main/operator*"
```

To execute tests natively (not in docker), you need to add `--native` parameter

If you need only one certain test, you may execute

```bash
python3 regression.py --only "/clickhouse_operator/test/main/operator/test_009*"
```

where `009` may be substituted by the number of the test you need. Tests --- numbers correspondence may be found in `tests/test.py` and `tests/test_operator.py` source code files.
