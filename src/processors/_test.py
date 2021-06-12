import argparse
import os
import json
from pathlib import Path
from datetime import datetime
from datetime import timedelta
import time
import sys


def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-minutes', type=int, default=1440, help='process duration (minutes)')
    return parser.parse_args()

args = set_args()

if args.root_path:
    os.chdir(f'{args.root_path}')
sys.path[0] = os.getcwd()

from common._mongodb_connector import MongoConnector
from common._s3_connector import S3Connector
from common._logger import get_logger

with open('../config.json') as f:
    config = json.load(f)


class TestClass(object):
    def __init__(self):
        self.mongo_conn = MongoConnector()
        self.s3_conn = S3Connector()
        logger.info('inner class test')


def main():
    test = TestClass()

    docu = test.mongo_conn.movies.find_one()
    test.mongo_conn.close()
    print(docu)

    test_path = 'test/test_docu.json'
    test.s3_conn.upload_to_s3_byte(docu, config['AWS']['S3_BUCKET'], test_path)
    loaded_docu = test.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], test_path)
    print(loaded_docu)



if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    start_time = time.time()
    logger.info(f'Process started at {datetime.now()}')
    print(f'Process started at {datetime.now()}')

    try:
        main()
    except Exception as e:
        logger.error(e)
    
    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process at {datetime.now()}. duration: {duration}.')
    print(f'Finishing process at {datetime.now()}. duration: {duration}.')