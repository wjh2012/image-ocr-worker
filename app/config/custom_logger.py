import inspect
import logging
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,  # 기본 레벨을 INFO로 설정
    format="%(asctime)s - %(message)s",  # 로그 형식 설정
    datefmt="%Y-%m-%d %H:%M:%S",
)


def time_logger(func):
    if inspect.iscoroutinefunction(func):

        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = await func(*args, **kwargs)
            end_time = time.perf_counter()

            elapsed_time = end_time - start_time
            log_message = f"{func.__name__}: {elapsed_time:.6f}sec"
            logging.info(log_message)

            return result

        return async_wrapper

    else:

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start_time = time.perf_counter()
            result = func(*args, **kwargs)
            end_time = time.perf_counter()

            elapsed_time = end_time - start_time
            log_message = f"{func.__name__}: {elapsed_time:.6f}sec"
            logging.info(log_message)

            return result

        return sync_wrapper
