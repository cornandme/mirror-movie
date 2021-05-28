from datetime import datetime
from datetime import timedelta
from io import BytesIO
import json
import logging
import pickle
import time

import boto3
import pandas as pd
from pymongo import MongoClient

with open('../config.json') as f:
    config = json.load(f)


class MovieInfoPreprocessor(object):
    def __init__(self):
        self._set_init()


    def _set_init(self):
        self.client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        self.db = self.client[config['DB']['DATABASE']]
        self.movies_df = pd.DataFrame(self.db[config['DB']['MOVIES']].find())
        self.makers_df = pd.DataFrame(self.db[config['DB']['MAKERS']].find())
        self.reviews_df = pd.DataFrame(self.db[config['DB']['USER_REVIEWS']].find())
        self.client.close()


    def filter_fault_rows(self):
        # filter
        self.movies_df = self.movies_df[~(
            self.movies_df['title_kor'].isna()
            | self.movies_df['poster_url'].isna()
            | self.movies_df['stillcut_url'].isna()
        )]
        return self


    def preprocess(self):   
        # 장르 필터
        self.movies_df = self.movies_df[~(
            self.movies_df['genre'].map(set(['에로']).issubset)
            | self.movies_df['genre'].map(set(['공연실황']).issubset)
        )]

        # 코멘트 통계 추가
        self.add_review_stat()

        # 코멘트 수 필터
        self.movies_df = self.movies_df[self.movies_df['review_count'] > 0]
        
        # 개봉년도 추출
        self.get_year_column()

        # 개봉일 수정
        self.compansate_release_date()
        
        # 스태프 컬럼 추가
        self.merge_staff_columns()

        # 필요 없는 컬럼 제거
        self.movies_df = self.movies_df.drop(columns=['updated_at', 'review_checked_date'])
        
        # 컬럼 이름 변경
        self.movies_df = self.movies_df.rename(columns={'_id': 'movie_id'})
        
        return self


    def add_review_stat(self):
        reviews_stat = self.reviews_df.groupby('movie_id').agg({'rate': ['mean', 'count']})
        merged = pd.merge(
            self.movies_df, 
            reviews_stat.rate, 
            how='left', 
            left_on='_id', 
            right_index=True, 
            validate='one_to_one'
        )
        merged = merged.rename(columns={
            'mean': 'avg_rate', 
            'count': 'review_count'
        })
        self.movies_df = merged
        return self


    def get_year_column(self):
        def year_process(row):
            date = row['release_date']
            if type(date) is float or len(date) < 4:
                result = row['title_eng'].split(',')[-1].replace(' ', '')
                return result if result.isdigit() else ''
            return date[:4]

        self.movies_df.loc[:, 'release_year'] = self.movies_df.apply(year_process, axis=1)
        return self

    def compansate_release_date(self):
        def date_process(row):
            date = row['release_date']
            year = row['release_year']
            if type(date) is float:
                try:
                    date = f'{year}0101'
                except Exception as e:
                    logger.error(e)
            return date

        self.movies_df.loc[:, 'release_date'] = self.movies_df.apply(date_process, axis=1)
        return self


    def merge_staff_columns(self):
        directors = self.makers_df[self.makers_df['role'] == 'director'][['movie_id', 'name', 'role']]
        writers = self.makers_df[self.makers_df['role'] == 'writer'][['movie_id', 'name', 'role']]

        merged1 = pd.merge(self.movies_df, directors, left_on='_id', right_on='movie_id', how='left', validate='one_to_one')
        merged2 = pd.merge(merged1, writers, left_on='_id', right_on='movie_id', how='left', validate='one_to_one')
        merged2 = merged2.rename(columns={'_id': 'movie_id', 'name_x': 'director', 'name_y': 'writer'})
        merged2 = merged2.drop(columns=['movie_id_x', 'role_x', 'movie_id_y', 'role_y'])

        self.movies_df = merged2
        return self


    def upload_to_s3(self):
        s3 = boto3.client(
            's3',
            aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
            aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
        )

        p = pickle.dumps(self.movies_df)
        file = BytesIO(p)
        s3.upload_fileobj(file, config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])

        return self


def main():
    processor = MovieInfoPreprocessor()
    processor.filter_fault_rows().preprocess().upload_to_s3()


if __name__=='__main__':
    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'./logs/movie_info_preprocess_{datetime.now().date()}.log', 
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    start_time = time.time()
    logger.info(f'Process started at {datetime.now()}')
    print(f'Process started at {datetime.now()}')

    try:
        main()
    except Exception as e:
        logger.error(e)
    
    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process at {datetime.now()}. duration: {duration}.')
    print(f'Finishing process at {datetime.now()}. duration: {duration}.')