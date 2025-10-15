import time
from functools import wraps
from collections import deque
import logging


logger = logging.getLogger(__name__)

class SensitiveStr(str):
    def __new__(cls, value):
        return super().__new__(cls, value)
    
    def __repr__(self):
        return "<SensitiveStr: ****>"
    
    def __str__(self):
        return "****"
    
    def reveal(self):
        return super().__str__()
    



GLOBAL_DOWNLOAD_TIMES = {}

def download_rate_limiter(resource_name, suggested_rps):
    """
    RPS limiter
    """
    
    if resource_name not in GLOBAL_DOWNLOAD_TIMES:
        GLOBAL_DOWNLOAD_TIMES[resource_name] = deque()

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            times = GLOBAL_DOWNLOAD_TIMES[resource_name]
            now = time.time()

            while times and now - times[0] > 1:
                times.popleft()

            if len(times) >= suggested_rps:
                earliest = times[0]
                sleep_time = 1 - (now - earliest)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                now = time.time()
                while times and now - times[0] > 1:
                    times.popleft()

            times.append(time.time())
            return func(*args, **kwargs)

        return wrapper
    return decorator