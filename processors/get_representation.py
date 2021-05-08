import argparse
from datetime import datetime
from datetime import timedelta
from functools import reduce
from io import BytesIO
import json
import logging
import multiprocessing as mp
import pickle
import time

import boto3
from gensim.models import FastText
import joblib
import numpy as np
from numpy.linalg import norm
import pandas as pd
import pymongo
from pymongo import MongoClient

with open("../config.json") as f:
    config = json.load(f)


class ReviewProcessor:
    def __init__(self, test):
        self.logger = logging.getLogger()
        self.test = test
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.morphs_df = None
        self.model = None
        self.movie_vectors = None
        self.clusters = None

        self.logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')


    def get_morphs(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]

        try:
            if self.test:
                morphs = db[config['DB']['USER_REVIEW_MORPHS']].find({'movie_id': {'$in': ['159074', '149777', '191637']}})
            else:
                morphs = db[config['DB']['USER_REVIEW_MORPHS']].find()
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
        df = pd.DataFrame(morphs)
        self.logger.info(f'got {len(df)} reviews.')
        print(f'got {len(df)} reviews.')
        self.morphs_df = df


    def build_model(self):
        morphs = self.morphs_df['morphs']

        model = FastText(
            vector_size=50, 
            window=7, 
            min_count=1,
            bucket=1500,
            workers=self.n_processes
        )

        model.build_vocab(corpus_iterable=morphs)

        model.train(
            corpus_iterable=morphs, 
            total_examples=len(morphs), 
            total_words=model.corpus_total_words, 
            epochs=10
        )

        self.model = model


    def save_model(self):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )

        trial = 0
        while True:
            try:
                with BytesIO() as f:
                    joblib.dump(self.model, f)
                    f.seek(0)
                    s3.upload_fileobj(f, config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])
                return
            except Exception as e:
                trial += 1
                self.logger.error(f'[trial {trial}]{e}')
                if trial > 2:
                    self.logger.critical('failed to save model!!')
                    break
                time.sleep(1)
                continue


    def make_movie_vectors(self):

        word_vectors = self.model.wv
        
        movie_vectors = pd.DataFrame()
        movie_vectors['movie_id'] = self.morphs_df['movie_id']
        # get averaged document vector
        movie_vectors.loc[:, 'vector'] = self.morphs_df['morphs'].map(lambda morphs: np.average([word_vectors[morph] for morph in morphs], axis=0))
        movie_vectors = movie_vectors.groupby('movie_id').sum()

        self.movie_vectors = movie_vectors


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


def main():
    processor = ReviewProcessor(test=args.test)
    processor.get_morphs()
    processor.build_model()
    processor.make_movie_vectors()
    processor.upload_file_to_s3(processor.model, config['MODEL']['MODEL_PATH'])
    processor.save_file(processor.model, config['PROCESS']['MODEL_PATH'])
    processor.save_file(processor.movie_vectors, config['PROCESS']['MOVIE_VECTORS_PATH'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-test', type=int, default=0, help='use small data set for test')
    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'./logs/review_process_{datetime.now().date()}.log', 
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'test: {args.test}')
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')