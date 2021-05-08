import argparse
from datetime import datetime
from datetime import timedelta
from io import BytesIO
import json
import logging
import pickle
import time

import boto3
import joblib
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import Normalizer

with open("../config.json") as f:
    config = json.load(f)



def main(n_clusters):
    # load data
    movie_vectors = pickle.load(open(config['PROCESS']['MOVIE_VECTORS_PATH'], 'rb'))


    # l2 Normalization
    X = Normalizer().fit_transform(list(movie_vectors['vector']))


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
    def upload_file_to_s3(file, s3_path):
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
                logger.error(f'[trial {trial}]{e}')
                if trial > 9:
                    logger.error('failed to upload files!!')
                    break
                time.sleep(1)
                continue

    upload_file_to_s3(movie_vectors, config['MODEL']['MOVIE_VECTORS_PATH'])
    upload_file_to_s3(cluster_df, config['MODEL']['CLUSTER_PATH'])

    pickle.dump(movie_vectors, open(config['PROCESS']['MOVIE_VECTORS_PATH'], 'wb'))
    pickle.dump(cluster_df, open(config['PROCESS']['CLUSTER_PATH'], 'wb'))


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-n_clusters', type=int, default=20, help='determine how many clusters are generated')
    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'./logs/clustering_{datetime.now().date()}.log', 
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'clustering started. {datetime.now()}')
    print(f'clustering started. {datetime.now()}')
    start_time = time.time()

    main(args.n_clusters)

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')