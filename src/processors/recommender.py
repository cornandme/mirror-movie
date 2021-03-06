import argparse
from datetime import datetime
from datetime import timedelta
import json
import multiprocessing as mp
import os
from pathlib import Path
import sys
import time

import numpy as np
import pandas as pd
from scipy.spatial.distance import cosine
from sklearn.cluster import KMeans
from sklearn.preprocessing import Normalizer

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-cores', type=int, default=0, help='how many cores ')
    parser.add_argument('-n_newest_rec', type=int, default=40)
    parser.add_argument('-n_cluster_rec', type=int, default=40)
    parser.add_argument('-n_genre_rec', type=int, default=40)
    parser.add_argument('-n_actor_rec', type=int, default=15)
    parser.add_argument('-n_director_rec', type=int, default=15)
    parser.add_argument('-n_similar_rec', type=int, default=30)
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


class Rec:
    def __init__(self, cores):
        self.mongo_conn = MongoConnector()
        self.s3_conn = S3Connector()
        self.n_processes = self._set_n_processes(cores)
        self.movies_df = self._load_movies_df()
        self.makers_df = self._load_makers_df()
        self.cluster_df = self._load_cluster_df()
        self.movie_vectors = self._load_movie_vectors()

        try:
            mp.set_start_method('spawn')
        except RuntimeError:
            pass


    def make_newest_rec(self, n):
        newest_df = self.movies_df.sort_values(by='release_date', ascending=False)
        newest_df = newest_df[['movie_id', 'poster_url']][:n]
        newest_rec = {'newest_rec': newest_df.to_dict('records')}

        self.s3_conn.upload_to_s3_byte(newest_rec, config['AWS']['S3_BUCKET'], config['REC']['FRONT_NEWEST'])


    def make_cluster_rec(self, n):
        cluster_movie_df = pd.merge(self.movie_vectors, self.movies_df, on='movie_id', sort=False, validate='one_to_one')
        cluster_movie_df = cluster_movie_df[['cluster', 'movie_id', 'vector', 'poster_url', 'avg_rate', 'review_count']]

        # ?????? ?????????
        cluster_movie_df = cluster_movie_df[
            (cluster_movie_df['review_count'] >= 50)
            & (cluster_movie_df['avg_rate'] >= 8)
        ]

        # ?????? ?????? ??????
        clusters = cluster_movie_df['cluster'].value_counts()
        clusters = clusters[clusters > 25]
        clusters = list(clusters.index)
        cluster_movie_df = cluster_movie_df[cluster_movie_df['cluster'].isin(clusters)]

        '''
        # ?????? ??????
        cluster_df = self.cluster_df[self.cluster_df.index.isin(clusters)].copy()
        X = Normalizer().fit_transform(list(cluster_df['vector']))

        def train_kmeans_model(X, k):
            model = KMeans(
                init='k-means++', 
                n_clusters=k, 
                max_iter=10000, 
                tol=1e-12
            ).fit(X)
            
            return model

        kmeans = train_kmeans_model(X, 20)
        cluster_df.loc[:, 'cluster_set'] = kmeans.labels_

        # ??????????????? ????????? ??????
        clusters = list(cluster_df.groupby('cluster_set').sample(n=1).index)
        '''

        # ?????? ????????? ????????? ????????? ??????
        cluster_movie_df = pd.merge(
            cluster_movie_df, 
            self.cluster_df, 
            left_on='cluster', 
            right_index=True, 
            copy=False, 
            validate='many_to_one'
        )
        cluster_movie_df.loc[:, 'vector_y'] = cluster_movie_df['vector_y'].map(lambda x: list(x))
        cluster_movie_df.loc[:, 'dist'] = cluster_movie_df.apply(lambda x: cosine(x.vector_x, x.vector_y), axis=1)
        cluster_movie_df = cluster_movie_df[['movie_id', 'cluster', 'dist']].reset_index()

        cluster_rec = dict()
        for cluster in clusters:
            cluster_rec[cluster] = cluster_movie_df[cluster_movie_df['cluster'] == cluster][['movie_id']][:n].to_dict('records')
        
        self.s3_conn.upload_to_s3_byte(cluster_rec, config['AWS']['S3_BUCKET'], config['REC']['FRONT_CLUSTER'])


    def make_genre_rec(self, n):
        genres = self.movies_df['genre'].explode().value_counts()
        genres = genres[genres >= n]
        genres = list(genres.index)
        genre_df = self.movies_df[['movie_id', 'poster_url', 'genre']]
        rows = genre_df.to_dict('records')

        # ???????????? ?????? ????????? ??????
        genre_rows = []
        for row in rows:
            for genre in row['genre']:
                genre_rows.append({
                    'movie_id': row['movie_id'],
                    'poster_url': row['poster_url'],
                    'genre': genre
                })
        genre_df = pd.DataFrame(genre_rows)

        # ??????
        genre_df = genre_df.sample(frac=1)
        
        # ???????????? ??????
        genre_rec = dict()
        for genre in genres:
            genre_rec[genre] = genre_df[genre_df['genre'] == genre][['movie_id', 'poster_url']][:n].to_dict('records')
        
        self.s3_conn.upload_to_s3_byte(genre_rec, config['AWS']['S3_BUCKET'], config['REC']['FRONT_GENRE'])


    def make_actor_rec(self, n):
        movie_ids = list(self.movies_df['movie_id'])
        movies_df = self.movies_df.set_index('movie_id').sort_index()
        makers_df = pd.merge(self.makers_df, self.movies_df[['movie_id']], on='movie_id', validate='many_to_one')
        makers_df = makers_df.set_index(['movie_id', 'maker_id']).sort_index()
        actor_rec = dict()

        for movie_id in movie_ids:
            # ????????? ?????????
            main_actor_ids = movies_df.loc[movie_id]['main_actor_ids']

            # ????????? ????????? ?????????
            actors_df = makers_df[
                ~makers_df.index.isin([movie_id], level='movie_id')
                & makers_df.index.isin(main_actor_ids, level='maker_id')
            ]
            actors_df.reset_index(inplace=True)
            
            # ???????????? ????????? - ??????x
            '''
            actors_df['maker_id'] = pd.Categorical(
                actors_df['maker_id'],
                categories=main_actor_ids,
                ordered=True
            )
            '''

            # ?????????????????? ??????
            actors_df = actors_df.sort_values(by=['release_date'], ascending=[False])

            # ?????? ?????? ??????
            actors_df = actors_df.drop_duplicates(subset=['movie_id'])

            # ?????????
            rec = actors_df[['movie_id', 'poster_url']][:n].to_dict('records')
            actor_rec[movie_id] = rec

        self.s3_conn.upload_to_s3_byte(actor_rec, config['AWS']['S3_BUCKET'], config['REC']['DETAIL_ACTOR'])


    def make_director_rec(self, n):
        movie_ids = list(self.movies_df['movie_id'])
        makers_df = pd.merge(self.makers_df, self.movies_df[['movie_id']], on='movie_id', validate='many_to_one')
        makers_df = makers_df.set_index(['movie_id', 'maker_id', 'role']).sort_index()
        director_rec = dict()

        for movie_id in movie_ids:
            # ??????, ??????
            directors = makers_df[
                makers_df.index.isin([movie_id], level='movie_id')
                & makers_df.index.isin(['director', 'writer'], level='role')
            ].index.get_level_values('maker_id').to_list()

            # ??????, ????????? ????????? ?????????
            directors_df = makers_df[
                ~makers_df.index.isin([movie_id], level='movie_id')
                & makers_df.index.isin(directors, level='maker_id')
            ]
            directors_df.reset_index(inplace=True)
            
            # ?????????????????? ??????
            directors_df = directors_df.sort_values(by=['release_date'], ascending=[False])

            # ?????? ?????? ??????
            directors_df = directors_df.drop_duplicates(subset=['movie_id'])

            # ?????????
            rec = directors_df[:n][['movie_id', 'poster_url']].to_dict('records')
            director_rec[movie_id] = rec
        
        self.s3_conn.upload_to_s3_byte(director_rec, config['AWS']['S3_BUCKET'], config['REC']['DETAIL_DIRECTOR'])


    def make_similar_rec(self, n):
        similar_rec_df = pd.merge(
            self.movie_vectors, 
            self.movies_df[self.movies_df['review_count'] >= 30][['movie_id', 'poster_url']], 
            on='movie_id', 
            sort=False, 
            validate='one_to_one'
        )
        movie_ids = list(self.movie_vectors.index)        

        similar_rec = dict()
        for movie_id in movie_ids:
            movie = self.movie_vectors.loc[movie_id]
            # ?????? ????????????????????? ????????? ????????? ????????? ??? ??????
            cluster_distances = self.cluster_df['vector'].map(lambda x: cosine(x, movie['vector'])).sort_values(ascending=True)
            clusters = list(cluster_distances.index)
            
            # ?????? ?????? ????????? ?????????????????? ????????? ?????? ????????? ?????? ???????????? ??????
            df_li = []
            movie_count = 0
            for cluster in clusters:
                tmp_df = similar_rec_df[similar_rec_df['cluster'] == cluster]
                df_li.append(tmp_df)
                movie_count += len(tmp_df)
                if movie_count >= 100: 
                    break

            similar_df = pd.concat(df_li)
            
            # ?????????????????? ??????
            distances = similar_df['vector'].map(lambda x: cosine(x, movie['vector']))
            similar_df = similar_df.assign(distance=distances).sort_values(by='distance')

            # ???????????? ??????
            similar_df = similar_df[similar_df['movie_id'] != movie.name]

            # n
            similar_df = similar_df[:n]

            # ??????
            rec = similar_df[['movie_id', 'poster_url']].to_dict('records')
            similar_rec[movie_id] = rec
        
        self.s3_conn.upload_to_s3_byte(similar_rec, config['AWS']['S3_BUCKET'], config['REC']['DETAIL_SIMILAR'])


    def _set_n_processes(self, cores):
        if (not cores) or (cores >= (mp.cpu_count() // 2)):
            return (mp.cpu_count() // 2) - 1
        return cores


    def _load_movies_df(self):
        movies_df = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])
        return movies_df


    def _load_makers_df(self):
        makers_df = pd.DataFrame(self.mongo_conn.makers.find())
        self.mongo_conn.close()
        # makers_df = makers_df[makers_df['movie_id'].isin(self.movies_df['movie_id'])]
        makers_df = makers_df.rename(columns={'movie_poster_url': 'poster_url'})
        return makers_df


    def _load_cluster_df(self):
        cluster_df = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['CLUSTER_PATH'])
        return cluster_df


    def _load_movie_vectors(self):
        movie_vectors = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])
        movie_vectors = movie_vectors[movie_vectors.index.isin(self.movies_df['movie_id'])]
        return movie_vectors


def main():
    rec = Rec(cores=args.cores)
    rec.make_newest_rec(n=args.n_newest_rec)
    rec.make_cluster_rec(n=args.n_cluster_rec)
    rec.make_genre_rec(n=args.n_genre_rec)
    rec.make_actor_rec(n=args.n_actor_rec)
    rec.make_director_rec(n=args.n_director_rec)
    rec.make_similar_rec(n=args.n_similar_rec)


if __name__ == '__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')