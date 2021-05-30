import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
from pathlib import Path
import time

import pandas as pd
from pymongo import MongoClient


class MorphExtractor:
    def __init__(self, pos_chunk, pos_target):
        self.logger = logging.getLogger()
        self.pos_chunk = pos_chunk
        self.pos_target = pos_target
        self._set_iter_count()

        print(pos_chunk)
        print(pos_target)


    def _set_iter_count(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS']]

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
        tokens = db[config['DB']['USER_REVIEW_TOKENS']]
        
        try:
            pos = tokens.find({'morphed': {'$in': [None, False]}})[:self.pos_chunk]
            pos_df = pd.DataFrame(pos)[['_id', 'movie_id', 'tokens']]
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()

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
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        tokens = db[config['DB']['USER_REVIEW_TOKENS']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS']]
        
        morphs_dict = self.morph_df.to_dict('records')
        try:
            for doc in morphs_dict:
                tokens.update_one({'_id': doc['_id']}, {'$set': {'morphed': True}})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
        self.morph_df = self.morph_df[self.morph_df['morphs'].astype('str') != '[]']
        morphs_dict = self.morph_df.to_dict('records')
        comment_count = len(morphs_dict)
        try:
            for doc in morphs_dict:
                morphs.insert_one(doc)
            self.logger.info(f'{comment_count} comments are morphed.')
            print(f'{comment_count} comments are morphed.')
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        

    def _target_row_count(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        pos_path = db[config['DB']['USER_REVIEW_TOKENS']]

        try:
            doc_count = pos_path.count_documents({})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
        return doc_count


def main():
    morph_extractor = MorphExtractor(pos_chunk=args.pos_chunk, pos_target=args.pos_target)
    
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
    parser.add_argument('-pos_target', type=str, nargs='+', default='NNG NNP VV VA MAG VX NF NV', help='pos list to extract')
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
    logger.info(f'get_morphs started. {datetime.now()}')
    print(f'get_morphs started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing get_morphs at {datetime.now()}. duration: {duration}.')
    print(f'Finishing get_morphs at {datetime.now()}. duration: {duration}.')
