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
from pandas.core.common import flatten

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
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


def main():
    mongo_conn = MongoConnector()
    s3_conn = S3Connector()

    def _load_movies_df():
        movies_df = s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])
        return movies_df

    def _load_makers_df():
        makers_df = pd.DataFrame(mongo_conn.makers.find())
        mongo_conn.close()
        
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
        cluster_df = s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['CLUSTER_PATH'])
        return cluster_df

    def _load_movie_vectors():
        movie_vectors = s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['MOVIE_VECTORS_PATH'])
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

    # unload
    s3_conn.upload_to_s3_byte(subword_hash, config['AWS']['S3_BUCKET'], config['DATA']['SUBWORD_HASH'])
    s3_conn.upload_to_s3_byte(name_id_hash, config['AWS']['S3_BUCKET'], config['DATA']['NAME_ID_HASH'])


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')