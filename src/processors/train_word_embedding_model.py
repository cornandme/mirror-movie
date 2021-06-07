import argparse
from datetime import datetime
from datetime import timedelta
from io import BytesIO
import json
import logging
import multiprocessing as mp
import os
from pathlib import Path
import pickle
import time

import boto3
import gensim
from gensim.models import FastText
import joblib
import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient


class WordEmbeddingModel:
    def __init__(self, all):
        self.logger = logging.getLogger()
        self.all = all
        self.n_processes = (mp.cpu_count() // 2) - 1
        self.morphs_df = None
        self.model = None

        if self.all:
            self.logger.info('using all sentences')
            print('using all sentences')
        self.logger.info(f'using {self.n_processes} cores')
        print(f'using {self.n_processes} cores')


    def get_morphs(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]

        try:
            if self.all:
                morphs = db[config['DB']['USER_REVIEW_MORPHS']].find()
            else:
                morphs = db[config['DB']['USER_REVIEW_MORPHS']].find({'fasttext_trained': {'$in': [None, False]}})
        except Exception as e:
            self.logger.error(e)
        finally:
            client.close()
        
        df = pd.DataFrame(morphs)
        self.logger.info(f'got {len(df)} reviews.')
        print(f'got {len(df)} reviews.')

        self.document_updated = len(df) > 0
        self.morphs_df = df


    def load_trained_model(self):
        if self.all:
            return

        try:
            model = self._load_from_s3(config['AWS']['S3_BUCKET'], config['MODEL']['MODEL_PATH'])
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
            model = FastText(
                vector_size=300, 
                window=7, 
                sg=1,
                negative=5,
                min_count=2,
                bucket=1500,
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
            epochs=1
        )

        self.model = model
        print('train model finished')


    def label_morphs_collection(self):
        client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        db = client[config['DB']['DATABASE']]
        morphs = db[config['DB']['USER_REVIEW_MORPHS']]

        try:
            for idx in range(len(self.morphs_df)):
                row = self.morphs_df.iloc[idx]
                morphs.update_one({'_id': row['_id']}, {'$set': {'fasttext_trained': True}})
        except Exception as e:
            self.logger.error(e)
        
        print('label finished')


    def save_file(self, file, path):
        try:
            pickle.dump(file, open(path, 'wb'))
        except Exception as e:
            self.logger.error(e)
    
    
    def upload_file_to_s3(self, file, s3_path):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )

        p = pickle.dumps(file)
        file = BytesIO(p)

        trial = 0
        while True:
            try:
                s3.upload_fileobj(file, config['AWS']['S3_BUCKET'], s3_path)
                return
            except Exception as e:
                trial += 1
                self.logger.error(f'[trial {trial}]{e}')
                if trial > 9:
                    self.logger.error('failed to upload files!!')
                    break
                time.sleep(1)
                continue          


    def _load_from_s3(self, bucket, path):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
            aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
        )

        with BytesIO() as f:
            p = s3.download_fileobj(bucket, path, f)
            f.seek(0)
            data = joblib.load(f)
        return data


def main():
    processor = WordEmbeddingModel(args.all)
    processor.get_morphs()
    if not processor.document_updated:
        print('No document updated. Stop process.')
        return
    processor.load_trained_model()
    processor.build_model()
    processor.upload_file_to_s3(processor.model, config['MODEL']['MODEL_PATH'])
    processor.save_file(processor.model, config['PROCESS']['MODEL_PATH'])
    processor.label_morphs_collection()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-all', type=bool, default=False, help='reset model and train new model with full sentences.')
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
    logger.info(f'process started. {datetime.now()}')
    print(f'process started. {datetime.now()}')
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. duration: {duration}.')
    print(f'Finishing process. duration: {duration}.')