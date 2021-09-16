# ClickHouse Operator tests

This folder contains TestFlows tests for ClickHouse Operator. This file describes how to launch those tests.

## Requirements

To execute tests, you will need:

- Python 3.8 or higher
- TestFlows Python library
- Docker and docker-compose

## Build test image

In order to run tests, you will need a base image. By default it will be pulled from GitLab registry. 

You may alse build the image locally. To do it, make the following steps:

```bash
cd image
./build_docker.sh
```

Be aware that building an image, as well as pulling it, will require about 5 GB of downloaded data.

## Execution

To execute the test suite (that currently involves only operator tests, not tests for third-party tools used by operator), execute the following command:

```bash
python3 regression.py --only "/clickhouse operator/test/main module/operator*"
```

If you need only one certain test, you may execute

```bash
python3 regression.py --only "/clickhouse operator/test/main module/operator/test_009*"
```

where `009` may be substituted by the number of the test you need. Tests --- numbers correspondence may be found in `tests/test.py` and `tests/test_operator.py` source code files.
