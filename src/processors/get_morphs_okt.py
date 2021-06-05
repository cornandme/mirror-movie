import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
from pathlib import Path
import time

from pymongo import MongoClient

import pandas as pd


class MorphExtractor:
    def __init__(self, pos_chunk, pos_target, all):
        self.logger = logging.getLogger()
        self.pos_chunk = pos_chunk
        self.pos_target = pos_target

        if all:
            print('reset morph data')
            self._reset_morph_data()

        self._set_iter_count()

        print(pos_chunk)
        print(pos_target)


    def _reset_morph_data(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS_OKT']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS_OKT']]
        okt_adjective_stat = db[config['DB']['OKT_ADJECTIVE_STAT']]

        try:
            morphs.drop()
            okt_adjective_stat.drop()
            tokens.update_many({}, {'$set': {'morphed': False}})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()


    def _set_iter_count(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS_OKT']]

        try:
            rows = tokens.count_documents({'morphed': {'$in': [None, False]}})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        self.iter = (rows // self.pos_chunk + 1) if rows > 0 else 0

    
    def get_pos(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS_OKT']]
        
        try:
            pos = tokens.find({'morphed': {'$in': [None, False]}})[:self.pos_chunk]
            pos_df = pd.DataFrame(pos)[['_id', 'movie_id', 'tokens', 'rate']]
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()

        print(len(pos_df))
        print(pos_df.iloc[0])
        self.pos_df = pos_df


    def get_morphs(self):
        self.pos_df.loc[:, 'nouns'] = self.pos_df['tokens'].map(lambda x: [pos[0] for pos in x if pos[1] == 'Noun'])
        self.pos_df.loc[:, 'adjectives'] = self.pos_df['tokens'].map(lambda x: [pos[0] for pos in x if pos[1] == 'Adjective'])

        morph_df = self.pos_df.drop(columns=['tokens'])
        self.morph_df = morph_df


    def save_morphs(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS_OKT']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS_OKT']]
        okt_adjective_stat = db[config['DB']['OKT_ADJECTIVE_STAT']]
        
        morphs_dict = self.morph_df.to_dict('records')
        comment_count = len(morphs_dict)
        try:
            for doc in morphs_dict:
                for adj in doc['adjectives']:
                    okt_adjective_stat.update_one({'_id': adj}, {'$set': {'_id': adj}, '$inc': {'count': 1}}, upsert=True)
                morphs.insert_one(doc)
                tokens.update_one({'_id': doc['_id']}, {'$set': {'morphed': True}})
            self.logger.info(f'{comment_count} comments are morphed.')
            print(f'{comment_count} comments are morphed.')
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        

    def _target_row_count(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        pos_path = db[config['DB']['USER_REVIEW_TOKENS_OKT']]

        try:
            doc_count = pos_path.count_documents({})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
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
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-pos_chunk', type=int, default=1000000, help='limit rows to process.')
    parser.add_argument('-pos_target', type=str, nargs='+', default='Noun Adjective', help='pos list to extract')
    parser.add_argument('-all', type=bool, default=False, help='reset all documents.')
    args = parser.parse_args()

    if args.root_path:
        os.chdir(f'{args.root_path}/processors')

    with open('../../config.json') as f:
        config = json.load(f)

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'../../logs/{Path(__file__).stem}_{datetime.now().date()}.log',
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'get_morphs_okt started. {datetime.now()}')
    print(f'get_morphs_okt started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing get_morphs_okt at {datetime.now()}. duration: {duration}.')
    print(f'Finishing get_morphs_okt at {datetime.now()}. duration: {duration}.')
