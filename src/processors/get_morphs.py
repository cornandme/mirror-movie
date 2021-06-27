import argparse
from datetime import datetime
from datetime import timedelta
import json
import os
from pathlib import Path
import sys
import time

import pandas as pd


def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-pos_chunk', type=int, default=1000000, help='limit rows to process.')
    parser.add_argument('-pos_target', type=str, nargs='+', default='NNG NNP VV VA MAG VX NF NV XR', help='pos list to extract')
    parser.add_argument('-all', type=bool, default=False, help='extract morphs from entire corpus.')
    return parser.parse_args()

args = set_args()

if args.root_path:
    os.chdir(f'{args.root_path}')
sys.path[0] = os.getcwd()

from common._mongodb_connector import MongoConnector
from common._logger import get_logger

with open('../config.json') as f:
    config = json.load(f)


class MorphExtractor:
    def __init__(self, pos_chunk, pos_target, all):
        self.mongo_conn = MongoConnector()
        self.pos_chunk = pos_chunk
        self.pos_target = pos_target
        self.all = all

        print(pos_chunk)
        print(pos_target)

        if self.all:
            logger.info('get morphs from entire corpus. dropping morphs collection.')
            self._drop_morphs()

        self._set_iter_count()


    def _drop_morphs(self):
        tokens = self.mongo_conn.user_review_tokens
        morphs = self.mongo_conn.user_review_morphs

        try:
            morphs.drop()
            morphs.create_index('movie_id')
            tokens.update_many({}, {'$set': {'morphed': False}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()


    def _set_iter_count(self):
        tokens = self.mongo_conn.user_review_tokens

        try:
            rows = tokens.count_documents({'morphed': {'$in': [None, False]}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        self.iter = (rows // self.pos_chunk + 1) if rows > 0 else 0

    
    def get_pos(self):
        tokens = self.mongo_conn.user_review_tokens
        
        try:
            pos = tokens.find({'morphed': {'$in': [None, False]}})[:self.pos_chunk]
            pos_df = pd.DataFrame(pos)[['_id', 'movie_id', 'tokens']]
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()

        print(len(pos_df))
        print(pos_df.iloc[0])
        self.pos_df = pos_df


    def get_morphs(self):
        if self.pos_target is None:
            self.pos_df.loc[:, 'morphs'] = self.pos_df['tokens'].map(lambda x: [pos[0] for pos in x])
        else:
            self.pos_df.loc[:, 'morphs'] = self.pos_df['tokens'].map(lambda x: [pos[0] for pos in x if pos[1] in self.pos_target])
        morph_df = self.pos_df.drop(columns=['tokens'])
        
        self.morph_df = morph_df


    def save_morphs(self):
        tokens = self.mongo_conn.user_review_tokens
        morphs = self.mongo_conn.user_review_morphs
        
        morphs_dict = self.morph_df.to_dict('records')
        try:
            for doc in morphs_dict:
                tokens.update_one({'_id': doc['_id']}, {'$set': {'morphed': True}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        
        self.morph_df = self.morph_df[self.morph_df['morphs'].astype('str') != '[]']
        morphs_dict = self.morph_df.to_dict('records')
        comment_count = len(morphs_dict)
        try:
            for doc in morphs_dict:
                morphs.replace_one({'_id': doc['_id']}, doc, upsert=True)
            logger.info(f'{comment_count} comments are morphed.')
            print(f'{comment_count} comments are morphed.')
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        

    def _target_row_count(self):
        pos_path = self.mongo_conn.user_review_tokens

        try:
            doc_count = pos_path.count_documents({})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        
        return doc_count


def main():
    morph_extractor = MorphExtractor(pos_chunk=args.pos_chunk, pos_target=args.pos_target, all=args.all)
    
    iter_count = 0
    while True:
        print(f'iter: {iter_count}')
        if iter_count >= morph_extractor.iter:
            return
        morph_extractor.get_pos()
        morph_extractor.get_morphs()
        morph_extractor.save_morphs()
        iter_count += 1


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'get_morphs started. {datetime.now()}')
    print(f'get_morphs started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing get_morphs at {datetime.now()}. duration: {duration}.')
    print(f'Finishing get_morphs at {datetime.now()}. duration: {duration}.')
