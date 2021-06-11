import argparse
from datetime import datetime
from datetime import timedelta
from io import BytesIO
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import pickle
from queue import Queue
import time

import boto3
import gensim
from gensim.models import FastText
import joblib
import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient


class MovieVectorProcessor:
    def __init__(self, chunk):
        self.logger = logging.getLogger()
        self.chunk = chunk
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.movie_id_q = Queue()
        self.model = None
        self.movie_vector_li = []
        self.morphs_df = None
        self.movie_vectors = None

        self._get_movie_ids()
        self._load_trained_model()

        self.logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')

    
    def _get_movie_ids(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS']]

        movie_ids = morphs.distinct('movie_id')

        for movie_id in movie_ids:
            self.movie_id_q.put(movie_id)


    def _load_trained_model(self):
        try:
            model = self._load_from_s3(config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])
        except Exception:
            model = None
        
        if self.__validate_model(model):
            self.model = model


    def __validate_model(self, model):
        return type(model) == gensim.models.fasttext.FastText


    def get_morphs(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS']]

        docu_count = 0
        df_li = []
        while (not self.movie_id_q.empty() and (docu_count < self.chunk)):
            movie_id = self.movie_id_q.get()
            try:
                morphs_df = pd.DataFrame(morphs.find({'movie_id': movie_id}, {'_id': 0, 'movie_id': 1, 'morphs': 1}))
                df_li.append(morphs_df)
                docu_count += len(morphs_df)
            except Exception as e:
                self.logger.error(e)
                client.close()
                
        client.close()
        
        self.logger.info(f'got {docu_count} reviews.')
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
        print('make movie vectors finished')

    
    def concat_vectors(self):
        self.movie_vectors = pd.concat(self.movie_vector_li)


    def save_file(self, file, path):
        try:
            pickle.dump(file, open(path, 'wb'))
        except Exception as e:
            self.logger.error(e)
    
    
    def upload_file_to_s3(self, file, s3_path):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )

        p = pickle.dumps(file)
        file = BytesIO(p)

        trial = 0
        while True:
            try:
                s3.upload_fileobj(file, config['AWS']['S3_BUCKET'], s3_path)
                return
            except Exception as e:
                trial += 1
                self.logger.error(f'[trial {trial}]{e}')
                if trial > 9:
                    self.logger.error('failed to upload files!!')
                    break
                time.sleep(1)
                continue          


    def _load_from_s3(self, bucket, path):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
            aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
        )

        with BytesIO() as f:
            p = s3.download_fileobj(bucket, path, f)
            f.seek(0)
            data = joblib.load(f)
        return data


def main():
    processor = MovieVectorProcessor(chunk=args.chunk)
    while not processor.movie_id_q.empty():
        processor.get_morphs()
        processor.make_movie_vectors()
    processor.concat_vectors()
    processor.save_file(processor.movie_vectors, config['PROCESS']['MOVIE_VECTORS_PATH'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-chunk', type=int, default=1000000, help='limit rows to process.')
    args = parser.parse_args()

    if args.root_path:
        os.chdir(f'{args.root_path}/processors')

    with open('../../config.json') as f:
        config = json.load(f)

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'../../logs/{Path(__file__).stem}_{datetime.now().date()}.log',
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')