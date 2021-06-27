import argparse
from datetime import datetime
from datetime import timedelta
import json
import multiprocessing as mp
import os
from pathlib import Path
import pickle
import sys
import time

import gensim
from gensim.models import FastText
import numpy as np
import pandas as pd

def set_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-all', type=bool, default=False, help='reset model and train new model with full sentences.')
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


class WordEmbeddingModel:
    def __init__(self, all):
        self.mongo_conn = MongoConnector()
        self.s3_conn = S3Connector()
        self.all = all
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.model_params = config['MODEL']['FASTTEXT_PARAMS']
        self.morphs_df = None
        self.model = None

        if self.all:
            logger.info('using all sentences')
            print('using all sentences')
        logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')


    def get_morphs(self):
        try:
            if self.all:
                morphs = self.mongo_conn.user_review_morphs.find()
            else:
                morphs = self.mongo_conn.user_review_morphs.find({'fasttext_trained': {'$in': [None, False]}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        
        df = pd.DataFrame(morphs)
        logger.info(f'got {len(df)} reviews.')
        print(f'got {len(df)} reviews.')

        self.document_updated = len(df) > 0
        self.morphs_df = df


    def load_trained_model(self):
        if self.all:
            return

        try:
            model = self.s3_conn.load_from_s3_byte(config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])
            logger.info('model loaded')
        except Exception:
            model = None
        
        if self._validate_model(model):
            self.model = model


    def _validate_model(self, model):
        return type(model) == gensim.models.fasttext.FastText


    def build_model(self):
        sentences = self.morphs_df['morphs']
        if len(sentences) == 0:
            return
        model = self.model

        if not model:
            logger.info('building new model.')
            logger.info(f'model params: {self.model_params}')

            model = FastText(
                vector_size=self.model_params['VECTOR_SIZE'], 
                window=self.model_params['WINDOW'], 
                sg=self.model_params['SG'],
                negative=self.model_params['NEGATIVE'],
                ns_exponent=self.model_params['NS_EXPONENT'],
                sample=self.model_params['SAMPLE'],
                min_n=self.model_params['MIN_N'],
                max_n=self.model_params['MAX_N'],
                min_count=self.model_params['MIN_COUNT'],
                bucket=self.model_params['BUCKET'],
                workers=self.n_processes
            )

            model.build_vocab(corpus_iterable=sentences)

        else:
            model.build_vocab(
                corpus_iterable=sentences,
                update=True
            )

        model.train(
            corpus_iterable=sentences, 
            total_examples=len(sentences), 
            epochs=self.model_params['EPOCHS']
        )

        self.model = model
        print('train model finished')


    def label_morphs_collection(self):
        morphs = self.mongo_conn.user_review_morphs

        try:
            for idx in range(len(self.morphs_df)):
                row = self.morphs_df.iloc[idx]
                morphs.update_one({'_id': row['_id']}, {'$set': {'fasttext_trained': True}})
        except Exception as e:
            logger.error(e)
        finally:
            self.mongo_conn.close()
        
        print('label finished')
    
    
    def upload_file_to_s3(self):
        self.s3_conn.upload_to_s3_byte(self.model, config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])       


def main():
    processor = WordEmbeddingModel(args.all)
    processor.get_morphs()
    if not processor.document_updated:
        print('No document updated. Stop process.')
        return
    processor.load_trained_model()
    processor.build_model()
    processor.upload_file_to_s3()
    processor.label_morphs_collection()


if __name__ == '__main__':
    logger = get_logger(filename=Path(__file__).stem)
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')