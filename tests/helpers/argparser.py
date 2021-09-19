import os

def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument("--clickhouse-image",
        type=str, dest="clickhouse_image",
        help="ClickHouse server docker image, default: yandex/clickhouse-server:21.8", metavar="path",
        default=os.getenv("CLICKHOUSE_IMAGE", "yandex/clickhouse-server:21.8"))

    parser.add_argument("--operator-version",
        type=str, dest="operator_version",
        help="ClickHouse Operator version, default: 0.15.0", metavar="path",
        default=os.getenv("OPERATOR_VERSION", "0.16.0"))

    parser.add_argument("--native",
        action="store_true",
        help="run tests on bare metal", default=False)

