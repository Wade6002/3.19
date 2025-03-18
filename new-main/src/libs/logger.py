import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

class EventHubLogger:
    _instances = {}

    def __new__(cls, name: str = "default"):
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(self, name: str = "default"):
        if not hasattr(self, '_initialized'):
            self.logger = logging.getLogger(name)
            self.logger.setLevel(logging.INFO)
            self._configure_handlers()
            self._initialized = True

    def _configure_handlers(self):
        formatter = logging.Formatter(
            '%(asctime)s - %(processName)s - %(levelname)s - %(message)s'
        )

        # 检查并创建日志目录
        log_dir = 'logs'
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        # 文件处理器（每个进程独立文件）
        file_handler = RotatingFileHandler(
            f'{log_dir}/{self.logger.name}.log',
            maxBytes=10 * 1024 * 1024,
            backupCount=5
        )
        file_handler.setFormatter(formatter)

        # 控制台处理器
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    @classmethod
    def get_logger(cls, name: Optional[str] = None) -> logging.Logger:
        return cls(name if name else "default").logger