import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
from pathlib import Path
import pickle
import sys
import time

from pymongo import MongoClient
import numpy as np
import pandas as pd

import boto3
from io import BytesIO
import joblib


def main():

    s3 = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
    )

    client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
    db = client[config['DB']['DATABASE']]

    def _load_from_s3(bucket, path):
        with BytesIO() as f:
            p = s3.download_fileobj(bucket, path, f)
            f.seek(0)
            data = joblib.load(f)
        return data

    def _load_movies_df():
        movies_df = _load_from_s3(config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])
        return movies_df

    def _load_makers_df():
        makers_df = pd.DataFrame(db[config['DB']['MAKERS']].find())
        makers_df = pd.merge(makers_df, movies_df[['movie_id', 'review_count']], on='movie_id', validate='many_to_one')
        roles = ['actor_main', 'director', 'writer', 'actor_sub']
        makers_df['role'] = pd.Categorical(
            makers_df['role'],
            categories=roles,
            ordered=True
        )
        makers_df = makers_df.rename(columns={'movie_poster_url': 'poster_url'})
        return makers_df

    def _load_cluster_df():
        with BytesIO() as f:
            p = s3.download_fileobj(config['AWS']['S3_BUCKET'], config['MODEL']['CLUSTER_PATH'], f)
            f.seek(0)
            cluster_df = joblib.load(f)
        return cluster_df

    def _load_movie_vectors():
        with BytesIO() as f:
            p = s3.download_fileobj(config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'], f)
            f.seek(0)
            movie_vectors = joblib.load(f)
        movie_vectors = movie_vectors[movie_vectors.index.isin(movies_df['movie_id'])]
        return movie_vectors

    movies_df = _load_movies_df()
    makers_df = _load_makers_df()
    cluster_df = _load_cluster_df()
    movie_vectors = _load_movie_vectors()

    # 리뷰 많은 순 정렬
    movies_df = movies_df.sort_values(by=['review_count'], ascending=False)

    # 역할, 리뷰 많은 순 정렬
    makers_df = makers_df.sort_values(by=['role', 'review_count'], ascending=[True, False])

    # 부분단어 해시
    def generate_hash(names):
        dic = dict()
        for name in names:
            if len(name) == 1:
                dic = _update_name_dic(name, name, dic)
                continue

            name_split = name.split(' ')
            name_split.append(name.replace(' ', ''))
            name_split.append(name)
            for word in name_split:
                length = len(word)
                if length < 2:
                    continue
                for i in range(2, length+1):
                    subword = word[:i]
                    dic = _update_name_dic(name, subword, dic)

        for key in dic.keys():
            dic[key] = get_unique_ordered_list(dic.get(key))
        return dic

    def _update_name_dic(name, word, dic):
        if dic.get(word) is None:
            dic[word] = []
        dic[word].append(name)
        return dic

    def get_unique_ordered_list(li):
        seen = set()
        return [x for x in li if not (x in seen or seen.add(x))]

    # 제목
    movie_names_kor = movies_df['title_kor']
    movie_names_hash = generate_hash(movie_names_kor)

    # 사람
    maker_names = makers_df['name']
    maker_names_hash = generate_hash(maker_names)

    # 장르
    from pandas.core.common import flatten
    genre_names = set(flatten(movies_df['genre']))
    genre_hash = generate_hash(genre_names)

    # 국가
    nation_names = set(flatten(movies_df['nations']))
    nation_names_hash = generate_hash(nation_names)

    # 병합
    subword_hash = dict()
    subword_hash['movie_name'] = movie_names_hash
    subword_hash['maker'] = maker_names_hash
    subword_hash['genre'] = genre_hash
    subword_hash['nation'] = nation_names_hash

    # 이름-영화id 해시
    def generate_name_id_hash(names, ids):
        dic = dict()
        for i in range(len(names)):
            if dic.get(names[i]) is None:
                dic[names[i]] = []
            dic[names[i]].append(ids[i])
        for key in dic.keys():
            dic[key] = get_unique_ordered_list(dic.get(key))
        return dic

    # 제목
    movie_names = list(movies_df['title_kor'])
    movie_ids = list(movies_df['movie_id'])
    movie_name_id_hash = generate_name_id_hash(movie_names, movie_ids)

    # 이름
    maker_names = list(makers_df['name'])
    maker_ids = list(makers_df['movie_id'])
    maker_id_hash = generate_name_id_hash(maker_names, maker_ids)

    # 장르
    ex_movies_df = movies_df[['movie_id', 'genre']].explode('genre')
    genres = list(ex_movies_df['genre'])
    genre_ids = list(ex_movies_df['movie_id'])
    genre_id_hash = generate_name_id_hash(genres, genre_ids)

    # 국가
    ex_movies_df = movies_df[['movie_id', 'nations']].explode('nations')
    nations = list(ex_movies_df['nations'])
    nation_ids = list(ex_movies_df['nations'])
    nation_id_hash = generate_name_id_hash(nations, nation_ids)

    # 병합
    name_id_hash = dict()
    name_id_hash['movie_name'] = movie_name_id_hash
    name_id_hash['maker'] = maker_id_hash
    name_id_hash['genre'] = genre_id_hash
    name_id_hash['nation'] = nation_id_hash


    def upload_to_s3(data, s3_path):
        p = pickle.dumps(data)
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


    upload_to_s3(subword_hash, config['DATA']['SUBWORD_HASH'])
    upload_to_s3(name_id_hash, config['DATA']['NAME_ID_HASH'])



if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    args = parser.parse_args()

    if args.root_path:
        os.chdir(f'{args.root_path}')
    sys.path[0] = os.getcwd()

    with open('../config.json') as f:
        config = json.load(f)

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'../logs/{Path(__file__).stem}_{datetime.now().date()}.log',
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