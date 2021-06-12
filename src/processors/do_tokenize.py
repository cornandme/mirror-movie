import argparse
from datetime import datetime
from datetime import timedelta
from functools import partial
import json
import os
from pathlib import Path
import sys
import multiprocessing as mp
import time

import numpy as np
import pandas as pd
from konlpy.tag import Komoran

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-cores', type=int, default=0, help='how many cores ')
    parser.add_argument('-rows', type=int, default=1000000, help='limit rows to process.')
    return parser.parse_args()

args = set_args()

if args.root_path:
    os.chdir(f'{args.root_path}')
sys.path[0] = os.getcwd()

from common._mongodb_connector import MongoConnector
from common._logger import get_logger

with open('../config.json') as f:
    config = json.load(f)



def _add_tokens(df):
    k = Komoran()
    df['tokens'] = df['review'].map(lambda x: _get_tokens(k, x))
    return df


def _get_tokens(k, string):
    try:
        result = k.pos(string)
        return result
    except:
        return []


def _filter_empty_token_row(df):
    return df[df['tokens'].astype('str') != '[]']


def _drop_columns(columns, df):
    return df.drop(columns=columns)


class Tokenizer(object):
    def __init__(self, cores, rows):
        self.mongo_conn = MongoConnector()
        self.n_processes = self._set_n_processes(cores)
        self.rows = rows

        logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')


    def get_reviews(self):
        user_reviews = self.mongo_conn.user_reviews
        try:
            reviews = user_reviews.find({'tokenized': {'$in': [None, False]}})
            if self.rows != 0:
                reviews = reviews[:self.rows]
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()

        df = pd.DataFrame(reviews)[['_id', 'movie_id', 'review']]
        
        logger.info(f'got {len(df)} reviews.')
        print(f'got {len(df)} reviews.')

        self._split_df(df)


    def tokenize(self):
        try:
            mp.set_start_method('spawn')
        except RuntimeError:
            pass

        with mp.Pool(processes=self.n_processes, maxtasksperchild=1) as p:
            for idx, chunk in enumerate(self.reviews_df_split):
                print(f'chunk {idx}: size {len(chunk)}')
                df_job = np.array_split(chunk, self.n_processes)
                print('getting tokens')
                df_job = p.map(_add_tokens, df_job)
                print('filtering empty rows')
                df_job = p.map(_filter_empty_token_row, df_job)
                print('dropping columns')
                func = partial(_drop_columns, ['review'])
                df_job = p.map(func, df_job)
                print('concatting chunk')
                tokens_df_chunk = pd.concat(df_job)
                print('updating result')
                self.reviews_df_split[idx] = tokens_df_chunk
        
        tokens_df = pd.concat(self.reviews_df_split)
        del self.reviews_df_split
        self.tokens_df = tokens_df

    
    def save_tokens(self):
        review_tokens = self.mongo_conn.user_review_tokens
        user_reviews = self.mongo_conn.user_reviews

        tokens_li = self.tokens_df.to_dict('records')
        try:
            for tokens in tokens_li:
                user_reviews.update_one({'_id': tokens['_id']}, {'$set': {'tokenized': True}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()

        self.tokens_df = self.tokens_df[self.tokens_df['tokens'].astype('str') != '[]']
        tokens_li = self.tokens_df.to_dict('records')
        try:
            for tokens in tokens_li:
                review_tokens.replace_one({'_id': tokens['_id']}, tokens, upsert=True)
            logger.info(f'{len(tokens_li)} comments are tokenized.')
            print(f'{len(tokens_li)} comments are tokenized.')
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()


    def _set_n_processes(self, cores):
        if (not cores) or (cores >= (mp.cpu_count() // 2)):
            return (mp.cpu_count() // 2) - 1
        return cores


    def _split_df(self, df):
        split_to = len(df) // 500000 + 1
        self.reviews_df_split = np.array_split(df, split_to)
        
        logger.info(f'splited to {split_to}')
        print(f'splited to {split_to}')


def process():
    tokenizer = Tokenizer(cores=args.cores, rows=args.rows)
    tokenizer.get_reviews()
    tokenizer.tokenize()
    tokenizer.save_tokens()


def job_estimater():
    mongo_conn = MongoConnector()
    user_reviews = mongo_conn.user_reviews

    row_count = user_reviews.count_documents({'tokenized': {'$in': [None, False]}})
    print(f'{row_count} rows are not tokenized.')

    if row_count == 0:
        print('all documents are tokenized. finish process.')
        return 0

    n_iters = row_count // args.rows + 1
    print(f'need process {n_iters} times')
    return n_iters
    

def main():
    n_iters = job_estimater()
    if n_iters == 0:
        return
    for n in range(n_iters):
        print(f'start process {n}')
        process()


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'tokenize started. {datetime.now()}')
    print(f'tokenize started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing tokenize. duration: {duration}.')
    print(f'Finishing tokenize. duration: {duration}.')

