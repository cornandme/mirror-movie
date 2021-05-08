from io import BytesIO
import json
import pickle

import boto3
import joblib
import pandas as pd
from pymongo import MongoClient


def main():
    with open('../config.json') as f:
        config = json.load(f)

    client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
    db = client[config['DB']['DATABASE']]

    # get raw data from db
    movies_df = pd.DataFrame(db[config['DB']['MOVIES']].find())
    makers_df = pd.DataFrame(db[config['DB']['MAKERS']].find())

    # process 1
    def merge_staff_columns(movies_df, makers_df):
        directors = makers_df[makers_df['role'] == 'director'][['movie_id', 'name', 'role']]
        writers = makers_df[makers_df['role'] == 'writer'][['movie_id', 'name', 'role']]

        merged1 = pd.merge(movies_df, directors, left_on='_id', right_on='movie_id', how='left', validate='one_to_one')
        merged2 = pd.merge(merged1, writers, left_on='_id', right_on='movie_id', how='left', validate='one_to_one')
        merged2 = merged2.rename(columns={'_id': 'movie_id', 'name_x': 'director', 'name_y': 'writer'})
        return merged2

    # process 2
    def get_year_column(df):
        def year_process(row):
            date = row['release_date']
            if len(date) == 0:
                result = row['title_eng'].split(',')[-1].replace(' ', '')
                return result if result.isdigit() else ''
            return date[:4]

        df.loc[:, 'release_year'] = df.apply(year_process, axis=1)
        return df

    # drop useless columns
    def drop_columns(df):
        df = df.drop(columns=['updated_at', 'movie_id_x', 'role_x', 'movie_id_y', 'role_y'])
        return df

    def filter_fault_rows(df):
        # filter
        df = df[~(df['title_kor'].isna()
                        | df['release_date'].isna()
                        | (df['release_date'] == '')
                        | (df['review_count'] == 0)
                        | df['poster_url'].isna()
                        | df['stillcut_url'].isna()
                        | (df['release_date'].str.len() > 10)
                        | (df['release_date'].str.len() < 4)
                        | (df['release_year'] == 'None')
                        | (df['review_count'] < 30)
                    )]
        df = df[~df['genre'].map(set(['에로']).issubset)]

        df = df.rename(columns={'_id': 'movie_id'})
        return df

    # execute
    result = merge_staff_columns(movies_df, makers_df)
    result = get_year_column(result)
    result = drop_columns(result)
    result = filter_fault_rows(result)

    # unload
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
    )

    p = pickle.dumps(result)
    file = BytesIO(p)
    s3.upload_fileobj(file, config['AWS']['S3_BUCKET'], config['DATA']['MOVIE_INFO'])

if __name__=='__main__':
    main()