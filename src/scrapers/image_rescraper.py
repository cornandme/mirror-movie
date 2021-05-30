import random
import json
import time
from datetime import timedelta
from urllib.error import HTTPError
from urllib.error import URLError

from bs4 import BeautifulSoup
import requests

import boto3
from pymongo import MongoClient

import numpy as np
import pandas as pd

with open("../config.json") as f:
    config = json.load(f)


class ImageRescraper(object):
    def __init__(self):
        self._init()
        
    def _init(self):
        self.config = config

        client = MongoClient(self.config["DB"]["DB_URL"], self.config["DB"]["DB_PORT"])
        self.db = client[self.config["DB"]["DATABASE"]]

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=self.config["AWS"]["AWS_SECRET_KEY"]
        )

        self.s3_resource = boto3.resource(
            's3',
            aws_access_key_id=self.config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=self.config["AWS"]["AWS_SECRET_KEY"]
        )
        self.bucket = self.s3_resource.Bucket(self.config['AWS']['S3_BUCKET'])

        self.session = requests.Session()
        self.headers = self.config["SCRAPER"]["HEADERS"]

    def get_movie_ids(self):
        # 1. db에서 데이터 가져와서 정제
        movies_df = pd.DataFrame(self.db[self.config['DB']['MOVIES']].find())
        movies_df = movies_df[~(
            movies_df['title_kor'].isna()
            | movies_df['poster_url'].isna()
            | movies_df['stillcut_url'].isna()
        )]
        self.movie_ids = list(movies_df['_id'])
        print(f'available movie count: {len(self.movie_ids)}')

    def get_target(self):
        # 2. s3에서 키셋 가져와서 딕셔너리로 변환
        poster_key = 'posters/'
        stillcut_key = 'stillcuts/'

        posters = [obj.key.replace(poster_key, '').replace('.jpg', '') for obj in list(self.bucket.objects.filter(Prefix=poster_key))]
        poster_dict = {key: True for key in posters}

        stillcuts = [obj.key.replace(stillcut_key, '').replace('.jpg', '') for obj in list(self.bucket.objects.filter(Prefix=stillcut_key))]
        stillcut_dict = {key: True for key in stillcuts}


        # 3. 이미지 누락된 영화 찾기
        target_movies = dict()
        for movie_id in self.movie_ids:
            no_poster = poster_dict.get(movie_id) is None
            no_stillcut = stillcut_dict.get(movie_id) is None
            if no_poster or no_stillcut:
                info = {'no_poster': no_poster, 'no_stillcut': no_stillcut}
                target_movies[movie_id] = info
        
        self.target_movies = target_movies
        print(f'image fault movie count: {len(self.target_movies.keys())}')

    def scrape_images(self):
        # 4. 이미지 재수집
        for movie_id in self.target_movies.keys():
            path = self.config["SCRAPER"]["NAVER_MOVIE_BASIC_PATH"] + movie_id
            html = self._get_html(path)
            soup = BeautifulSoup(html, 'html.parser')

            if self.target_movies[movie_id]['no_poster']:
                try:
                    poster_url = soup.find('div', {'class': 'poster'}).find('img')['src']
                    poster_url = poster_url[:poster_url.index('=')+1] + 'm203_290_2'
                    poster_s3_path = f"posters/{movie_id}.jpg"
                    self._upload_to_s3(poster_url, poster_s3_path)
                    time.sleep(1)
                except Exception as e:
                    print(e)
                    pass

            if self.target_movies[movie_id]['no_stillcut']:
                try:
                    stillcut_url = soup.find('div', {'class': 'viewer_img'}).find('img')['src']
                    stillcut_s3_path = f"stillcuts/{movie_id}.jpg"
                    self._upload_to_s3(stillcut_url, stillcut_s3_path)
                    time.sleep(1)
                except Exception as e:
                    print(e)
                    pass


    def _get_html(self, path):
        self.headers['path'] = path
        try:
            html = self.session.get(path, headers=self.headers, timeout=30).text
        except HTTPError as e:
            print(e)
        except URLError as e:
            print(e)
        return html

    def _upload_to_s3(self, url_from, path_to):
        trial = 0
        while True:
            try:
                with requests.get(url_from, stream=True) as r:
                    self.s3_client.upload_fileobj(
                        r.raw, 
                        self.config['AWS']['S3_BUCKET'], 
                        path_to
                    )
                break
            except Exception as e:
                trial += 1
                print(f'[trial {trial}]{e}')
                time.sleep(1)
                if trial >= 10:
                    break
                continue


def sleep():
    return time.sleep(config["SCRAPER"]["MOVIE_INFO_SLEEP_INTERVAL"] - random.random()*0.2)

def main():
    image_rescraper = ImageRescraper()
    image_rescraper.get_movie_ids()
    image_rescraper.get_target()
    image_rescraper.scrape_images()


if __name__=='__main__':
    start_time = time.time()

    main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    print(f'Finishing process. duration: {duration}.')