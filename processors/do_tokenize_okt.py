import argparse
from datetime import datetime
from datetime import timedelta
from functools import partial
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import time

from pymongo import MongoClient

import numpy as np
import pandas as pd
from konlpy.tag import Okt


class Tokenizer(object):
    def __init__(self, test, cores, rows):
        self.logger = logging.getLogger()
        self.test = test
        self.n_processes = self._set_n_processes(cores)
        self.rows = rows

        if test:
            self.logger.info('*** test ***')
            print('*** test ***')
        self.logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')


    def get_reviews(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        user_reviews = db[config['DB']['USER_REVIEWS']]

        try:
            reviews = user_reviews.find({'tokenized_okt': {'$in': [None, False]}})
            if self.rows != 0:
                reviews = reviews[:self.rows]
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()

        df = pd.DataFrame(reviews)[['_id', 'movie_id', 'review']]
        
        self.logger.info(f'got {len(df)} reviews.')
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
                df_job = p.map(self._add_tokens, df_job)
                print('filtering empty rows')
                df_job = p.map(self._filter_empty_token_row, df_job)
                print('dropping columns')
                func = partial(self._drop_columns, ['review'])
                df_job = p.map(func, df_job)
                print('concatting chunk')
                tokens_df_chunk = pd.concat(df_job)
                print('updating result')
                self.reviews_df_split[idx] = tokens_df_chunk
        
        tokens_df = pd.concat(self.reviews_df_split)
        del self.reviews_df_split
        self.tokens_df = tokens_df

    
    def save_tokens(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        if self.test:
            db = client[config['DB']['TEST_DATABASE']]
        else:
            db = client[config['DB']['DATABASE']]
        review_tokens = db[config['DB']['USER_REVIEW_TOKENS_OKT']]
        user_reviews = db[config['DB']['USER_REVIEWS']]

        tokens_li = self.tokens_df.to_dict('records')
        try:
            if self.test:
                pass
            else:
                for tokens in tokens_li:
                    if tokens['tokens'] != []:
                        review_tokens.insert_one(tokens)
                    user_reviews.update_one({'_id': tokens['_id']}, {'$set': {'tokenized_okt': True}})
                self.logger.info(f'{len(tokens_li)} comments are tokenized.')
                print(f'{len(tokens_li)} comments are tokenized.')
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()


    def _set_n_processes(self, cores):
        if (not cores) or (cores >= (mp.cpu_count() // 2)):
            return (mp.cpu_count() // 2) - 1
        return cores


    def _split_df(self, df):
        split_to = len(df) // 500000 + 1
        self.reviews_df_split = np.array_split(df, split_to)
        
        self.logger.info(f'splited to {split_to}')
        print(f'splited to {split_to}')


    def _add_tokens(self, df):
        okt = Okt()
        df['tokens'] = df['review'].map(lambda x: self._get_tokens(okt, x))
        return df


    def _get_tokens(self, okt, string):
        try:
            result = okt.pos(string, norm=True, stem=True)
            result = [token for token in result if len(token) == 2]
            return result
        except Exception as e:
            self.logger.error(e)
            return []


    def _filter_empty_token_row(self, df):
        return df[df['tokens'].astype('str') != '[]']


    def _drop_columns(self, columns, df):
        return df.drop(columns=columns)


def process():
    tokenizer = Tokenizer(test=args.test, cores=args.cores, rows=args.rows)
    tokenizer.get_reviews()
    tokenizer.tokenize()
    tokenizer.save_tokens()


def job_estimater():
    client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
    db = client[config['DB']['DATABASE']]
    user_reviews = db[config['DB']['USER_REVIEWS']]


    row_count = user_reviews.count_documents({'tokenized_okt': {'$in': [None, False]}})
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-test', type=bool, default=0, help='use small data set for test')
    parser.add_argument('-cores', type=int, default=0, help='how many cores ')
    parser.add_argument('-rows', type=int, default=1000000, help='limit rows to process.')
    args = parser.parse_args()

    if args.root_path:
        os.chdir(f'{args.root_path}/processors')

    with open('../config.json') as f:
        config = json.load(f)

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'../logs/{Path(__file__).stem}_{datetime.now().date()}.log',
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'test: {args.test}')
    logger.info(f'tokenize started. {datetime.now()}')
    print(f'tokenize started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing tokenize. duration: {duration}.')
    print(f'Finishing tokenize. duration: {duration}.')

