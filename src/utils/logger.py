"""
封装Logger模块,增加了容器内的logs文件输出和warnning和error时的调用栈打印
"""

import sys
import logging
from pathlib import Path


class LoggerWrapper:
    """
    一个日志记录器的封装类，旨在简化日志配置并为错误日志自动添加堆栈跟踪。
    """

    def __init__(self, name, level=logging.DEBUG, log_file=None):
        """
        初始化日志记录器。

        :param name: 日志记录器的名称，通常使用 __name__。
        :param level: 日志记录的级别，默认为 DEBUG。
        :param log_file: (可选) 日志输出的文件路径。如果指定，日志将同时输出到控制台和文件。
        """
        # 1. 创建一个 logger 实例
        # getLogger(name) 返回一个具有指定名称的 logger 实例，如果不存在则会创建。
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 2. 创建一个 Formatter 来定义日志格式
        # %(asctime)s: 日志创建时间
        # %(name)s: logger 的名称
        # %(levelname)s: 日志级别 (e.g., DEBUG, INFO, ERROR)
        # %(message)s: 日志消息
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # 3. 创建并配置 Handlers
        # 避免重复添加 handlers
        if not self.logger.handlers:
            # --- 控制台日志处理器 ---
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

            final_log_path = None
            if log_file is False:
                # 禁用文件日志
                pass
            elif isinstance(log_file, str):
                # 用户提供了自定义路径
                final_log_path = Path(log_file)
            else:  # log_file is None (默认情况)
                # 使用 pathlib 获取项目根目录 (即当前工作目录)
                project_root = Path.cwd()
                # 定义默认的日志路径
                log_dir = project_root / "logs"
                # 创建目录 (如果不存在)
                log_dir.mkdir(parents=True, exist_ok=True)
                # 定义完整的日志文件路径
                final_log_path = log_dir / f"{name}.log"

            # 如果最终的日志路径有效，则添加 FileHandler
            if final_log_path:
                # 'a' 表示追加模式, 'utf-8' 确保正确处理各种字符
                file_handler = logging.FileHandler(
                    final_log_path, mode="a", encoding="utf-8"
                )
                file_handler.setFormatter(formatter)
                self.logger.addHandler(file_handler)

    def debug(self, msg, *args, **kwargs):
        """记录一条 DEBUG 级别的日志。"""
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        """记录一条 INFO 级别的日志。"""
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        """记录一条 WARNING 级别的日志。"""
        self.logger.error("%s%s%s", "━━" * 10, "WARNING" * 4, "━━" * 10)
        self.logger.warning(msg, *args, exc_info=True, **kwargs)

    def error(self, msg, *args, **kwargs):
        """
        记录一条 ERROR 级别的日志。
        关键点：在调用 logger.error 时，我们设置 exc_info=True。
        这会使得 logging 模块自动捕获并记录异常堆栈信息。
        这个方法应该在 `except` 块中调用，以正确捕获异常。
        """
        # exc_info=True 会自动添加异常信息到日志消息中 [7, 8]
        self.logger.error("%s%s%s", "━━" * 10, "ERROR" * 4, "━━" * 10)
        self.logger.error(msg, *args, exc_info=True, **kwargs)

    def critical(self, msg, *args, **kwargs):
        """
        记录一条 CRITICAL 级别的日志，同样自动附带堆栈信息。
        """
        self.logger.error("%s%s%s", "━━" * 10, "we are done" * 4, "━━" * 10)    # ! 这种基本基本都是很严重的程度，但是agent服务做好权限管理的话，一般不会用的这个
        self.logger.critical(msg, *args, exc_info=True, **kwargs)


logger = LoggerWrapper(__name__)