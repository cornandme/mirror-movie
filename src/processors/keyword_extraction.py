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
from scipy.stats import entropy

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-df_floor', type=int, default=30)
    parser.add_argument('-keyword_length', type=int, default=4)
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


class KeywordExtractor(object):
    def __init__(self):
        self.mongo_conn = MongoConnector()
        self.s3_conn = S3Connector()
        self.cluster_rec = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['REC']['FRONT_CLUSTER'])
        self.cluster_dic = self._make_cluster_dic()


    def _make_cluster_dic(self):
        cluster_dic = dict()
        for key, entry in self.cluster_rec.items():
            ids = [dic['movie_id'] for dic in entry]
            cluster_dic[key] = ids
        return cluster_dic


    def get_morphs(self):
        try:
            morphs = self.mongo_conn.user_review_morphs_okt.find({}, {'_id': 0, 'movie_id': 1, 'adjectives': 1})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()

        self.morphs_df = pd.DataFrame(morphs).set_index('movie_id').sort_index()
        self.morphs_df = self.morphs_df.rename(columns={'adjectives': 'morphs'})
        
        print(f'got {len(self.morphs_df)} comments.')
        return self

    
    def make_df_dic(self):
        movie_word_set = self.morphs_df.groupby('movie_id')['morphs'].apply(lambda x: np.unique(np.hstack(x)))
        
        df_dic = dict()
        for idx in range(len(movie_word_set)):
            for word in movie_word_set.iloc[idx]:
                if df_dic.get(word) is None:
                    df_dic[word] = 0
                df_dic[word] += 1
        
        self.df_dic = df_dic
        return self
    

    def make_movie_prob_distribution(self, df_floor):
        self.morphs_df.loc[:, 'morphs'] = self.morphs_df['morphs'].map(
            lambda x: [word for word in x if self.df_dic.get(word) >= df_floor]
        )

        self.morphs_df.loc[:, 'prob_dist'] = self.morphs_df['morphs'].map(
            lambda morphs: self._tf_to_prob_dist(self._make_tf_dic(morphs))
        )

        self.morphs_df = self.morphs_df.drop(columns=['morphs'])

        self.morphs_df = self.morphs_df.groupby(self.morphs_df.index).agg(
            {'prob_dist': lambda col: self._agg_to_movie(col)}
        )

        self.morphs_df = pd.DataFrame(
            data=self.morphs_df['prob_dist'].to_list(),
            index=self.morphs_df.index
        ).fillna(0)

        return self


    def extract_cluster_keywords(self, keyword_length):
        uni_entropy_series = self.morphs_df.apply(lambda x: entropy(x))
        uni_entropy_series.sort_values(ascending=False)

        cluster_entropy_dic = self._make_cluster_entropy_dic(self.cluster_dic, self.morphs_df)
        cluster_score_dic = self._make_cluster_score_dic(cluster_entropy_dic, uni_entropy_series)
        self.cluster_keyword_dic = self._make_cluster_keyword_dic(cluster_score_dic, keyword_length)

        return self


    def tag_keywords(self):
        cluster_rec = dict()
        for keywords in self.cluster_keyword_dic:
            rec = self.cluster_rec[self.cluster_keyword_dic[keywords]]
            cluster_rec[keywords] = rec
        
        self.cluster_rec = cluster_rec
        return self


    def upload_cluster_rec(self):
        self.s3_conn.upload_to_s3_byte(self.cluster_rec, config['AWS']['S3_BUCKET'], config['REC']['FRONT_CLUSTER'])


    def _make_tf_dic(self, words):
        tf_dic = dict()
        for word in words:
            tf_dic = self._update_tf_dic(word, tf_dic)
        return tf_dic


    def _update_tf_dic(self, word, dic):
        if dic.get(word) is None:
            dic[word] = 0
        dic[word] += 1
        return dic


    def _tf_to_prob_dist(self, dic):
        total_freq = sum(dic.values())
        return {key: (item / total_freq) for key, item in dic.items()}


    def _agg_to_movie(self, col):
        size = len(col)
        agg_dic = dict()
        for dic in col:
            agg_dic.update(dic)
        
        return {key: (value / size) for key, value in agg_dic.items()}

    
    def _make_cluster_entropy_dic(self, cluster_dic, morphs):
        cluster_entropy_dic = dict()
        for key, entry in cluster_dic.items():
            cluster_morphs = morphs[morphs.index.isin(entry)]
            entropy_series = cluster_morphs.apply(lambda x: entropy(x))
            cluster_entropy_dic[key] = entropy_series
        return cluster_entropy_dic


    def _make_cluster_score_dic(self, cluster_entropy_dic, uni_entropy_series):
        score_dic = dict()
        for key, ent in cluster_entropy_dic.items():
            score_dic[key] = (ent / uni_entropy_series).replace([np.inf, -np.inf], np.nan).dropna()
        return score_dic

    
    def _make_cluster_keyword_dic(self, cluster_score_dic, keyword_length):
        cluster_keyword_dic = dict()
        for key, keyword_series in cluster_score_dic.items():
            keyword_li = keyword_series.sort_values(ascending=False)[:keyword_length].index.to_list()
            keyword_li = [f'#{keyword}' for keyword in keyword_li]
            keywords = ' '.join(keyword_li)
            cluster_keyword_dic[keywords] = key
        return cluster_keyword_dic


def main():
    keyword_extractor = KeywordExtractor()
    keyword_extractor.get_morphs() \
        .make_df_dic() \
        .make_movie_prob_distribution(df_floor=args.df_floor) \
        .extract_cluster_keywords(keyword_length=args.keyword_length) \
        .tag_keywords() \
        .upload_cluster_rec()


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'keyword_extraction started. {datetime.now()}')
    print(f'keyword_extraction started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing keyword_extraction. duration: {duration}.')
    print(f'Finishing keyword_extraction. duration: {duration}.')