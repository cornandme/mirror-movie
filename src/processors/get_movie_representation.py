import argparse
from datetime import datetime
from datetime import timedelta
import json
import multiprocessing as mp
import os
from pathlib import Path
from queue import Queue
import sys
import time

import gensim
from gensim.models import FastText
import numpy as np
import pandas as pd

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-chunk', type=int, default=1000000, help='limit rows to process.')
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


class MovieVectorProcessor:
    def __init__(self, chunk):
        self.mongo_conn = MongoConnector()
        self.s3_conn = S3Connector()
        self.chunk = chunk
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.movie_id_q = Queue()
        self.model = None
        self.movie_vector_li = []
        self.morphs_df = None
        self.movie_vectors = None

        self._get_movie_ids()
        self._load_trained_model()

        logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')

    
    def _get_movie_ids(self):
        morphs = self.mongo_conn.user_review_morphs

        movie_ids = morphs.distinct('movie_id')

        for movie_id in movie_ids:
            self.movie_id_q.put(movie_id)
        
        self.mongo_conn.close()


    def _load_trained_model(self):
        try:
            model = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])
        except Exception:
            model = None
        
        if self.__validate_model(model):
            self.model = model


    def __validate_model(self, model):
        return type(model) == gensim.models.fasttext.FastText


    def get_morphs(self):
        morphs = self.mongo_conn.user_review_morphs

        docu_count = 0
        df_li = []
        while (not self.movie_id_q.empty() and (docu_count < self.chunk)):
            movie_id = self.movie_id_q.get()
            try:
                morphs_df = pd.DataFrame(morphs.find({'movie_id': movie_id}, {'_id': 0, 'movie_id': 1, 'morphs': 1}))
                df_li.append(morphs_df)
                docu_count += len(morphs_df)
            except Exception as e:
                logger.error(e)
                self.mongo_conn.close()
                
        self.mongo_conn.close()
        
        logger.info(f'got {docu_count} reviews.')
        print(f'got {docu_count} reviews.')

        self.morphs_df = pd.concat(df_li)


    def make_movie_vectors(self):

        word_vectors = self.model.wv
        
        movie_vectors = pd.DataFrame()
        movie_vectors['movie_id'] = self.morphs_df['movie_id']

        # get averaged comment vector
        movie_vectors.loc[:, 'vector'] = self.morphs_df['morphs'].map(lambda morphs: np.average([word_vectors[morph] for morph in morphs], axis=0))
        
        # get movie vector
        movie_vectors = movie_vectors.groupby('movie_id').sum()

        self.movie_vector_li.append(movie_vectors)

        logger.info('make movie vectors finished')
        print('make movie vectors finished')

    
    def concat_vectors(self):
        self.movie_vectors = pd.concat(self.movie_vector_li)         


def main():
    processor = MovieVectorProcessor(chunk=args.chunk)
    while not processor.movie_id_q.empty():
        processor.get_morphs()
        processor.make_movie_vectors()
    processor.concat_vectors()
    processor.s3_conn.upload_to_s3_byte(processor.movie_vectors, config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])


if __name__ == '__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')