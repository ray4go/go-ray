import argparse
import logging

from . import init, utils

logger = logging.getLogger(__name__)


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
        utils.get_module(import_file)

    ray_init_args = {}
    if args.mode == "cluster":
        ray_init_args = dict(address="auto")
    # args.debug
    init(args.go_binary_path, py_module_path=import_file, **ray_init_args)


if __name__ == "__main__":
    main()
