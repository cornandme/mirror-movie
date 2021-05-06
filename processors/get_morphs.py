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
    def __init__(self, pos_chunk=0):
        self.logger = logging.getLogger()
        self.target_row_count = self._target_row_count()
        self.pos_chunk = pos_chunk
        self.current_row = 0
        self.morphs = pd.Series(dtype='object')

    
    def get_pos(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        pos_path = db[config['DB']['USER_REVIEW_TOKENS']]

        row_to = self.current_row + self.pos_chunk
        print(f'getting pos from {self.current_row} to {row_to}')
        
        try:
            pos = pos_path.find()[self.current_row:row_to]
            pos_series = pd.DataFrame(pos)['tokens']
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()

        self.current_row = row_to
        self.pos_series = pos_series


    def get_morphs(self):
        new_morphs = self.pos_series.map(lambda x: [pos[0] for pos in x])
        self.morphs = pd.concat([self.morphs, new_morphs]) 


    def save_morphs(self):
        morphs_dict = {'morphs': self.morphs.to_list()}
        pickle.dump(morphs_dict, open('morphs.pickle', 'wb'))
        

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
    morph_extractor = MorphExtractor(pos_chunk=args.pos_chunk)
    
    while True:
        if morph_extractor.current_row > morph_extractor.target_row_count:
            break
        morph_extractor.get_pos()
        morph_extractor.get_morphs()
    
    morph_extractor.save_morphs()


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-pos_chunk', type=int, default=1000000, help='limit rows to process.')
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
