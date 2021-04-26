import argparse
from datetime import datetime
from datetime import timedelta
from functools import reduce
from functools import wraps
from io import BytesIO
import json
import logging
import multiprocessing as mp
import pickle
import random
import time

import boto3
from gensim.models import FastText
import joblib
from konlpy.tag import Komoran
import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient
from scipy.spatial.distance import cosine
from sklearn.cluster import KMeans
from sklearn.preprocessing import Normalizer

with open("../config.json") as f:
    config = json.load(f)


class ReviewProcessor:
    def __init__(self, test):
        self.logger = logging.getLogger()
        self.reviews = self._get_reviews(test)
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.model = None
        self.movie_vectors = None
        self.clusters = None

        self.logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')

    def tokenize(self):
        df_split = np.array_split(self.reviews, self.n_processes)

        with mp.Pool(processes=self.n_processes) as p:
            df_split = p.map(self._add_tokens, df_split)
            df_split = p.map(self._filter_empty_token_row, df_split)
            self.reviews = pd.concat(df_split)

    def build_model(self):
        model = FastText(vector_size=10, window=3, min_count=1, workers=self.n_processes)
        model.build_vocab(corpus_iterable=self.reviews['tokens'])
        model.train(corpus_iterable=self.reviews['tokens'], 
                    total_examples=len(self.reviews['tokens']), 
                    total_words=model.corpus_total_words, 
                    epochs=10)
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
        movie_vectors['movie_id'] = self.reviews['movie_id']
        movie_vectors['vector'] = self.reviews['tokens'].map(lambda tokens: reduce(np.add, [word_vectors[token] for token in tokens]))
        movie_vectors = movie_vectors.groupby('movie_id').sum()

        self.movie_vectors = movie_vectors
        
    def make_cluster(self):
        # l2 Normalization
        X = Normalizer().fit_transform(list(self.movie_vectors['vector']))

        # train kmeans model
        n_clusters = max((len(self.movie_vectors) // 100, 2))
        print(f'n_clusters: {n_clusters}')
        kmeans = KMeans(init='k-means++', n_clusters=n_clusters).fit(X)

        # cluster vector
        cluster_centroids = {idx: vector for idx, vector in enumerate(kmeans.cluster_centers_)}
        cluster_df = pd.DataFrame()
        cluster_df['vector'] = cluster_centroids.values()
        cluster_df.set_index(pd.Index(cluster_centroids.keys()))
        self.clusters = cluster_df

        # map cluster-movie
        self.movie_vectors['cluster'] = kmeans.labels_
        self.movie_vectors.sort_values(by=['cluster'])
    
    def upload_file_to_s3(self):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )

        p1 = pickle.dumps(self.clusters)
        p2 = pickle.dumps(self.movie_vectors)

        file1 = BytesIO(p1)
        file2 = BytesIO(p2)

        trial = 0
        while True:
            try:
                s3.upload_fileobj(file1, config['AWS']['S3_BUCKET'], config['MODEL']['CLUSTER_PATH'])
                s3.upload_fileobj(file2, config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])
                return
            except Exception as e:
                trial += 1
                self.logger.error(f'[trial {trial}]{e}')
                if trial > 2:
                    self.logger.error('failed to upload trained files!!')
                    break
                time.sleep(1)
                continue
        
    def _get_reviews(self, test):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]

        try:
            if test:
                reviews = db[config['DB']['USER_REVIEWS']].find({'movie_id': {'$in': ['159074', '149777', '191637']}})
            else:
                reviews = db[config['DB']['USER_REVIEWS']].find()
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
        df = pd.DataFrame(reviews)
        self.logger.info(f'got {len(df)} reviews.')
        print(f'got {len(df)} reviews.')
        return df
        
    def _add_tokens(self, df):
        k = Komoran()
        df['tokens'] = df['review'].map(lambda x: self._get_token(k, x))
        return df

    def _get_token(self, k, string):
        try:
            return k.nouns(string)
        except Exception as e:
            self.logger.error(e)
            return []

    def _filter_empty_token_row(self, df):
        return df[df['tokens'].astype('str') != '[]']


def main():
    processor = ReviewProcessor(test=args.test)
    processor.tokenize()
    processor.build_model()
    processor.save_model()
    processor.make_movie_vectors()
    processor.make_cluster()
    processor.upload_file_to_s3()


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