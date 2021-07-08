from abc import ABCMeta
from functools import wraps
from io import BytesIO
import time

import joblib

class S3Loader(metaclass=ABCMeta):
    def __init__(self, s3, config):
        self.s3 = s3
        self.config = config

    def _try_n_times(func, n=10):
        @wraps(func)
        def wrapper(*args, **kwargs):
            trials = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    print(f'trials {trials}: {e}')
                    trials += 1
                    if trials >= n:
                        break
                    time.sleep(3)
                    continue
        return wrapper

    @_try_n_times
    def load_from_s3(self, bucket, path):
        with BytesIO() as f:
            p = self.s3.download_fileobj(bucket, path, f)
            f.seek(0)
            data = joblib.load(f)
        return data