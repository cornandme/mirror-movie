import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import pickle
import time

from pymongo import MongoClient

import pandas as pd

with open('../config.json') as f:
    config = json.load(f)


class MorphExtractor:
    def __init__(self, pos_chunk, pos_target):
        self.logger = logging.getLogger()
        self.target_row_count = self._target_row_count()
        self.pos_chunk = pos_chunk
        self.pos_target = pos_target
        self.current_row = 0

        print(pos_chunk)
        print(pos_target)


    def clean_morph_db(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS']]

        try:
            morphs.drop()
            if morphs.count_documents() != 0:
                self.logger.error(f'db is not empty!!')
                raise Exception
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
    
    def get_pos(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        pos_path = db[config['DB']['USER_REVIEW_TOKENS']]

        row_to = self.current_row + self.pos_chunk
        print(f'getting pos from {self.current_row} to {row_to}')
        
        try:
            pos = pos_path.find()[self.current_row:row_to]
            pos_df = pd.DataFrame(pos)[['movie_id', 'tokens']]
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()

        self.current_row = row_to
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
                tokens.update_one({'_id': doc['_id']}, {'$set': {'morphed': True}}, upsert=True)
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
    morph_extractor.clean_morph_db()
    
    while True:
        if morph_extractor.current_row > morph_extractor.target_row_count:
            break
        morph_extractor.get_pos()
        morph_extractor.get_morphs()
        morph_extractor.save_morphs()


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pos_chunk', type=int, default=1000000, help='limit rows to process.')
    parser.add_argument('-pos_target', type=str, nargs='+', default='NNG NNP VV VA MAG VX NF NV', help='pos list to extract')
    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'./logs/get_morphs_{datetime.now().date()}.log', 
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    logger.info(f'get_morphs started. {datetime.now()}')
    print(f'get_morphs started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing get_morphs. duration: {duration}.')
    print(f'Finishing get_morphs. duration: {duration}.')
