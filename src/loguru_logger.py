import inspect
import sys
import time
from loguru import logger

logger.remove()

logger.add(
    "logs/main.log",
    rotation="100 MB",
    enqueue=True,
    compression="zip",
    backtrace=True,
    diagnose=True,
    encoding="utf-8"
)

logger.add(
    sys.stderr,
    level="DEBUG",
    enqueue=True,
    format="{time:HH:mm:ss} | {level} | {message}",
    backtrace=True,
    diagnose=True
)

def async_log_execution_time(func):
    async def wrapper(*args, **kwargs):
        start_time = time.time()  # 記錄開始時間
        result = await func(*args, **kwargs)  # 異步等待函數完成
        end_time = time.time()  # 記錄結束時間
        duration = end_time - start_time  # 計算耗時
        class_name = func.__qualname__.split('.')[0]  # 獲取類名
        func_name = func.__name__
        logger.info(f"Async function '{func_name}' in class '{class_name}' executed in {duration:.4f} seconds")
        return result
    return wrapper