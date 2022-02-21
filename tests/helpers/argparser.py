def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument(
        "--native",
        action="store_true",
        help="run tests without docker-compose, require only worked kubectl + python",
        default=False
    )
    parser.add_argument(
        "--keeper-type",
        type=str,
        help="which type of keeper will use for test",
        choices=["zookeeper", "clickhouse-keeper"],
        default="zookeeper"
    )
