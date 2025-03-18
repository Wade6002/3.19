# ==== file: libs/exceptions.py ====
from functools import wraps
from typing import Callable, Any

class EventHubError(Exception):
    """事件中心基础异常"""
    def __init__(self, message: str, context: dict = None):
        super().__init__(message)
        self.context = context or {}

class ConnectivityError(EventHubError):
    """连接相关异常"""
    pass

class ProcessingError(EventHubError):
    """消息处理异常"""
    pass
class ConfigValidationError(Exception):
    """配置验证异常基类"""
    pass

def error_handler(max_retries: int = 3) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            retries = 0
            while retries < max_retries:
                try:
                    return await func(*args, **kwargs)
                except ConnectivityError as e:
                    args[0].logger.error(
                        f"Connection error (attempt {retries+1}/{max_retries}): {str(e)}",
                        extra=e.context
                    )
                    retries += 1
                except ProcessingError as e:
                    args[0].logger.error(
                        f"Processing failed: {str(e)}",
                        extra=e.context
                    )
                    raise
            raise ConnectivityError(f"Max retries ({max_retries}) exceeded")
        return wrapper
    return decorator