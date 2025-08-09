import argparse
import contextlib
import importlib.util
import logging
import os

from . import init

logger = logging.getLogger(__name__)


def _get_module(path):
    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    # https://stackoverflow.com/questions/41861427/python-3-5-how-to-dynamically-import-a-module-given-the-full-file-path-in-the

    @contextlib.contextmanager
    def add_to_path(p):
        import sys

        sys.path.append(p)
        try:
            yield
        finally:
            sys.path.remove(p)

    # import_name will be the `__name__` of the imported module
    import_name = "__goray__"
    with add_to_path(os.path.dirname(path)):
        spec = importlib.util.spec_from_file_location(
            import_name, path, submodule_search_locations=None
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    return module


def main():
    """
    仅会在ray driver上被调用
    """
    parser = argparse.ArgumentParser(
        description="Python driver for ray-core-go application.",
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
        help="指定运行模式：\n"
        "  cluster: 在集群模式下运行\n"
        "  local: 在ray本地模式下运行",
    )
    parser.add_argument(
        "go_binary_path",  # 位置参数
        type=str,
        help="指定 Goray 应用二进制文件的路径 (使用 go build -buildmode=c-shared 构建)",
    )
    args = parser.parse_args()

    import_file = getattr(args, "import", None)
    if import_file:
        _get_module(import_file)

    ray_init_args = {}
    if args.mode == "cluster":
        ray_init_args = dict(address="auto")

    init(args.go_binary_path, ray_init_args, args.debug)


if __name__ == "__main__":
    main()
