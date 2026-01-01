import argparse
import logging
import sys

from . import start, common

logger = logging.getLogger(__name__)

helper = """\
GoRay application runner.

By default, it will autodetect an existing Ray cluster or start a new Ray instance if no existing cluster is found.
Use `--cluster` to explicitly connect to an existing local cluster.
"""


def main():
    parser = argparse.ArgumentParser(
        description=helper,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    # parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--cluster",
        action="store_true",
        help="To explicitly connect to an existing local cluster.",
    )

    parser.add_argument(
        "--py-defs",
        type=str,
        help="Path to a python file, where defines python ray tasks and actors to be used in GoRay application.",
    )

    parser.add_argument(
        "go_binary_path",  # positional argument
        type=str,
        help="Path to the GoRay binary file (built from `go build -buildmode=c-shared`)",
    )
    args = parser.parse_args()

    py_defs_file = args.py_defs or ""
    if py_defs_file:
        common.get_module(py_defs_file)

    ray_init_args = {}
    if args.cluster:
        ray_init_args = dict(address="auto")

    ret = start(args.go_binary_path, py_defs_path=py_defs_file, **ray_init_args)
    sys.exit(ret)


if __name__ == "__main__":
    main()
