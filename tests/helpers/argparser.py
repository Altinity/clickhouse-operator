def argparser(parser):
    """Default argument parser for regressions.
    """
    parser.add_argument(
        "--native",
        action="store_true",
        help="run tests without docker-compose, require only worked kubectl + python",
        default=False
    )
