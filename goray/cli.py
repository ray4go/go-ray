import argparse
import logging

from . import init, utils

logger = logging.getLogger(__name__)


def main():
    """
    only called on ray driver process
    """
    parser = argparse.ArgumentParser(
        description="GoRay application Runner",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logging")
    parser.add_argument(
        "--import",
        type=str,
        help="Specify a python script to import python ray tasks and actors",
    )

    parser.add_argument(
        "--mode",
        type=str,
        choices=["cluster", "local"],
        default="cluster",
        help="Ray cluster mode or local mode (default: cluster)",
    )
    parser.add_argument(
        "go_binary_path",  # positional argument
        type=str,
        help="Path to the GoRay binary file (built from `go build -buildmode=c-shared`)",
    )
    args = parser.parse_args()

    import_file = getattr(args, "import") or ""
    if import_file:
        utils.get_module(import_file)

    ray_init_args = {}
    if args.mode == "cluster":
        ray_init_args = dict(address="auto")
    # args.debug
    init(args.go_binary_path, py_module_path=import_file, **ray_init_args)


if __name__ == "__main__":
    main()
