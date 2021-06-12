from functools import wraps
from io import BytesIO
import json
import pickle
import time

import boto3
import joblib
import requests

with open('../config.json') as f:
    config = json.load(f)


class S3Connector(object):
    def __init__(self):
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )


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
    def load_from_s3_byte(self, bucket, path_from):
        with BytesIO() as f:
            p = self.s3.download_fileobj(bucket, path_from, f)
            f.seek(0)
            data = joblib.load(f)
        return data


    @_try_n_times
    def upload_to_s3_byte(self, file, bucket, path_to):
        p = pickle.dumps(file)
        file = BytesIO(p)
        self.s3.upload_fileobj(file, bucket, path_to)

    
    @_try_n_times
    def upload_to_s3_from_url(self, url_from, bucket, path_to):
        with requests.get(url_from, stream=True) as r:
            self.s3.upload_fileobj(
                r.raw,
                bucket,
                path_to
            )