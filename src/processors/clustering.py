import argparse
from datetime import datetime
from datetime import timedelta
import json
import os
from pathlib import Path
import sys
import time

import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import Normalizer

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-n_clusters', type=int, default=200, help='determine how many clusters are generated')
    return parser.parse_args()

args = set_args()

if args.root_path:
    os.chdir(f'{args.root_path}')
sys.path[0] = os.getcwd()

from common._s3_connector import S3Connector
from common._logger import get_logger

with open('../config.json') as f:
    config = json.load(f)


def main(n_clusters):
    s3_conn = S3Connector()

    # load data
    movie_vectors = s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])
    movies_df = s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])

    # filter movie
    movie_vectors = movie_vectors[
        movie_vectors.index.isin(
            movies_df[movies_df['review_count'] >= 30]['movie_id']
        )
    ]

    # train
    X = Normalizer(norm='l2').fit_transform(list(movie_vectors['vector']))

    def train_kmeans_model(X, k):
        model = KMeans(
            init='k-means++', 
            n_clusters=k, 
            max_iter=10000, 
            tol=1e-12
        ).fit(X)
        
        return model

    kmeans = train_kmeans_model(X, n_clusters)

    # cluster vector
    cluster_centroids = {idx: vector for idx, vector in enumerate(kmeans.cluster_centers_)}
    cluster_df = pd.DataFrame()
    cluster_df['vector'] = cluster_centroids.values()
    cluster_df = cluster_df.set_index(pd.Index(cluster_centroids.keys()))

    # map cluster-movie
    movie_vectors['cluster'] = kmeans.labels_
    movie_vectors = movie_vectors.sort_values(by=['cluster'])

    # unload
    s3_conn.upload_to_s3_byte(movie_vectors, config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])
    s3_conn.upload_to_s3_byte(cluster_df, config['AWS']['S3_BUCKET'], config['MODEL']['CLUSTER_PATH'])


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'clustering started. {datetime.now()}')
    print(f'clustering started. {datetime.now()}')
    start_time = time.time()

    main(args.n_clusters)

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')