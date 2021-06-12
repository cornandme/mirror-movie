import argparse
from datetime import datetime
from datetime import timedelta
import json
import os
from pathlib import Path
import re
import sys
import time

import pandas as pd

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-pos_chunk', type=int, default=1000000, help='limit rows to process.')
    parser.add_argument('-pos_target', type=str, nargs='+', default='Noun Adjective', help='pos list to extract.')
    parser.add_argument('-all', type=bool, default=False, help='convert adjectives in all documents.')
    return parser.parse_args()

args = set_args()

if args.root_path:
    os.chdir(f'{args.root_path}')
sys.path[0] = os.getcwd()

from common._mongodb_connector import MongoConnector
from common._logger import get_logger

with open('../config.json') as f:
    config = json.load(f)


class MorphPostProcessor(object):
    def __init__(self, pos_chunk, pos_target, all):
        self.mongo_conn = MongoConnector()
        self.pos_chunk = pos_chunk
        self.pos_target = pos_target
        self.adj_converter = self._load_adj_converter()
        self._set_iter_count()
        
        if all:
            print('reset update_checker')
            self._reset_update_checker()

    
    def _reset_update_checker(self):
        morphs = self.mongo_conn.user_review_morphs_okt
        
        try:
            morphs.update_many({}, {'$set': {'adj_converted': False}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()


    def _load_adj_converter(self):
        with open('adj_converter.json', 'rb') as f:
            adj_converter = json.load(f)
        return adj_converter


    def _set_iter_count(self):
        morphs = self.mongo_conn.user_review_morphs_okt

        try:
            rows = morphs.count_documents({'adj_converted': {'$in': [None, False]}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        self.iter = (rows // self.pos_chunk + 1) if rows > 0 else 0

    
    def get_adjs(self):
        morphs = self.mongo_conn.user_review_morphs_okt

        try:
            adjs = morphs.find(
                {'adj_converted': {'$in': [None, False]}}, 
                {'_id': 1, 'adjectives': 1}
            )[:self.pos_chunk]
            adj_df = pd.DataFrame(adjs)
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        
        print(len(adj_df))
        print(adj_df.iloc[0])
        self.adj_df = adj_df

        return self
    

    def convert_adjs(self):
        self.adj_df.loc[:, 'adjectives'] = self.adj_df['adjectives'].map(lambda x: self._convert_adjs(x))
        return self
    

    def _convert_adjs(self, adjs):
        return [self._convert_adj(adj) for adj in adjs if self._convert_adj(adj) is not None]
    

    def _convert_adj(self, adj):
        if adj in self.adj_converter['stopwords']:
            return
        if re.match('.*하다$', adj):
            return self.adj_converter['converter_hada'].get(adj) or f'{adj[:-2]}한'
        return self.adj_converter['converter'].get(adj) or adj

    
    def save_adjs(self):
        morphs = self.mongo_conn.user_review_morphs_okt

        try:
            for idx in range(len(self.adj_df)):
                doc = self.adj_df.iloc[idx]
                morphs.update_one(
                    {'_id': doc['_id']},
                    {
                        '$set': {
                            'adjectives': doc['adjectives'],
                            'adj_converted': True
                        }
                    }
                )
            print(f'{len(self.adj_df)} docs are converted.')
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
    

def main():
    morph_postprocessor = MorphPostProcessor(pos_chunk=args.pos_chunk, pos_target=args.pos_target, all=args.all)

    iter_count = 0
    while True:
        print(f'iter: {iter_count}')
        if iter_count >= morph_postprocessor.iter:
            return
        morph_postprocessor.get_adjs().convert_adjs().save_adjs()
        iter_count += 1


if __name__=='__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'morphs_okt_postprocess started. {datetime.now()}')
    print(f'morphs_okt_postprocess started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing morphs_okt_postprocess at {datetime.now()}. duration: {duration}.')
    print(f'Finishing morphs_okt_postprocess at {datetime.now()}. duration: {duration}.')