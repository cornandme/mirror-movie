import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import queue
import random
import re
import time
from urllib.error import HTTPError
from urllib.error import URLError

from bs4 import BeautifulSoup
import requests
import pandas as pd

import boto3
import pymongo
from pymongo import MongoClient

with open("../config.json") as f:
    config = json.load(f)


class MovieScraper:
    def __init__(self):
        self.logger = logging.getLogger()
        self.client = MongoClient(config["DB"]["DB_URL"], config["DB"]["DB_PORT"])
        self.db = self.client[config["DB"]["DATABASE"]]
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=config["AWS"]["AWS_ACCESS_KEY"],
            aws_secret_access_key=config["AWS"]["AWS_SECRET_KEY"]
        )
        self.session = requests.Session()
        self.headers = config["SCRAPER"]["HEADERS"]
        self.queue = self._set_queue()
        self.current_target = None
        self.movie = dict()
        self.makers = []

    def get_makers(self):
        return self.makers

    def set_movie_id(self):
        self.current_target = self.queue.get()
        self.movie['_id'] = self.current_target
        self.logger.info(f"Start processing movie_id: {self.movie['_id']}")

    def scrape_basic(self):
        path = config["SCRAPER"]["NAVER_MOVIE_BASIC_PATH"] + self.current_target
        html = self._get_html(path)
        soup = BeautifulSoup(html, 'html.parser')

        try:
            title_tags = soup.find('div', {'class': 'mv_info'})
            self.movie['title_kor'] = title_tags.find('h3', {'class': 'h_movie'}).find('a', {'href': f"./basic.nhn?code={self.movie['_id']}"}).get_text(strip=True)
            self.movie['title_eng'] = title_tags.find('strong', {'class': 'h_movie2'}).get_text(strip=True).replace('\r', '').replace('\t', '').replace('\n', '')
        except Exception as e:
            self.logger.warning(e)
            pass

        try:
            meta_tags = soup.find('dl', {'class': 'info_spec'})
            genre_tags = meta_tags.find_all('a', {'href': re.compile('genre=')})
            self.movie['genre'] = list({tag.get_text(strip=True) for tag in genre_tags})
        except Exception as e:
            pass

        try:
            self.movie['grade'] = meta_tags.find('a', {'href': re.compile('grade=')}).get_text(strip=True)
        except Exception as e:
            pass
        
        try:
            tags = meta_tags.find_all('span')
            texts = [tag.get_text(strip=True) for tag in tags]
            p = re.compile('^[0-9]{1,4}분$')
            for text in texts:
                if p.match(text):
                    running_time = text
                    break
            self.movie['running_time'] = running_time or None
        except Exception as e:
            pass
        
        try:
            nation_tag_li = meta_tags.find_all('a', href=re.compile('nation='))
            self.movie['nations'] = list({tag.get_text(strip=True) for tag in nation_tag_li})
        except Exception as e:
            pass
        
        try:
            release_tags = meta_tags.find_all('a', {'href': re.compile('open=')})
            p = re.compile('^[0-9]{4}$|^[0-9]{6}$|^[0-9]{8}$')
            for tag in release_tags[::-1]:
                href = tag['href']
                text = href[href.index('=')+1:]
                if p.match(text):
                    release_date = text
                    break
            self.movie['release_date'] = release_date or None
        except Exception as e:
            pass

        try:
            self.movie['story'] = soup.find('div', {'class': 'story_area'}).p.get_text().replace('\xa0', '\n')
        except Exception as e:
            pass

        try:
            poster_url = soup.find('div', {'class': 'poster'}).find('img')['src']
            poster_url = poster_url[:poster_url.index('=')+1] + 'm203_290_2'
            poster_s3_path = f"posters/{self.movie['_id']}.jpg"
            poster_s3_url = f'https://{config["AWS"]["S3_BUCKET"]}.s3.{config["AWS"]["S3_BUCKET_REGION"]}.amazonaws.com/{poster_s3_path}'
            self.movie['poster_url'] = poster_s3_url
        except Exception as e:
            self.logger.warning(e)
            pass
        
        if self.movie.get('poster_url') != None:
            self._upload_to_s3(poster_url, poster_s3_path)

        try:
            stillcut_url = soup.find('div', {'class': 'viewer_img'}).find('img')['src']
            stillcut_s3_path = f"stillcuts/{self.movie['_id']}.jpg"
            stillcut_s3_url = f'https://{config["AWS"]["S3_BUCKET"]}.s3.{config["AWS"]["S3_BUCKET_REGION"]}.amazonaws.com/{stillcut_s3_path}'
            self.movie['stillcut_url'] = stillcut_s3_url
        except Exception as e:
            self.logger.warning(e)
            pass

        if self.movie.get('stillcut_url') != None:
            self._upload_to_s3(stillcut_url, stillcut_s3_path)
        
        self.movie['updated_at'] = datetime.now()
        
    def scrape_detail(self):
        path = config["SCRAPER"]["NAVER_MOVIE_DETAIL_PATH"] + self.current_target
        html = self._get_html(path)
        soup = BeautifulSoup(html, 'html.parser')
        
        maker_li = []
        
        try:
            people_tags = soup.find('div', {'class': re.compile('section_group section_group_frst')})
            actor_tags = people_tags.find('ul', {'class': 'lst_people'}).find_all('div', {'class': 'p_info'})
        except Exception as e:
            self.logger.warning(e)
            pass

        main_actors = []
        main_actor_ids = []
        sub_actors = []
        sub_actor_ids = []

        # actors
        try:
            for tag in actor_tags:
                maker_url = tag.find('a')['href']
                idx = maker_url.index('=')
                
                maker_id = maker_url[idx+1:]
                name = tag.find('a').get_text(strip=True)
                part = tag.find('em', {'class': 'p_part'}).get_text(strip=True)
                
                # construct maker document
                role = 'actor_main' if part == '주연' else 'actor_sub'
                maker_li.append({
                    '_id': f"{maker_id}_{self.movie['_id']}_{role}",
                    'maker_id': maker_id,
                    'name': name,
                    'movie_id': self.movie['_id'], 
                    'movie_poster_url': self.movie['poster_url'], 
                    'role': role,
                    'release_date': self.movie['release_date']
                })
                
                if part == '주연':
                    main_actors.append(name)
                    main_actor_ids.append(maker_id)
                else:
                    sub_actors.append(name)
                    sub_actor_ids.append(maker_id)
        except Exception as e:
            self.logger.warning(e)
            pass

        # director
        try:
            dir_tag = people_tags.find('div', {'class': 'director'}).find('div', {'class': 'dir_product'}).find('a')
            director_name = dir_tag.get_text(strip=True)
            director_url = dir_tag['href']
            idx = maker_url.index('=')
            director_id = director_url[idx+1:]

            maker_li.append({
                '_id': f"{director_id}_{self.movie['_id']}_director",
                'maker_id': director_id,
                'name': director_name,
                'movie_id': self.movie['_id'],
                'movie_poster_url': self.movie['poster_url'],
                'role': 'director',
                'release_date': self.movie['release_date']
            })
        except Exception as e:
            self.logger.warning(e)
            pass

        # writer
        try:
            writer_tag = people_tags.find('div', {'class': 'staff'}).find('tbody').find('td').find('span').find('a')
            writer_url = writer_tag['href']
            idx = writer_url.index('=')
            writer_id = writer_url[idx+1:]
            writer_name = writer_tag.get_text(strip=True)

            maker_li.append({
                '_id': f"{writer_id}_{self.movie['_id']}_writer",
                'maker_id': writer_id,
                'name': writer_name,
                'movie_id': self.movie['_id'],
                'movie_poster_url': self.movie['poster_url'],
                'role': 'writer',
                'release_date': self.movie['release_date']
            })
        except Exception as e:
            self.logger.warning(e)
            pass

        self.movie['main_actors'] = main_actors
        self.movie['main_actor_ids'] = main_actor_ids
        self.movie['sub_actors'] = sub_actors
        self.movie['sub_actor_ids'] = sub_actor_ids
        self.makers = maker_li

    def upsert_data_to_db(self):
        try:
            self.db[config["DB"]["MOVIES"]].replace_one({'_id': self.movie['_id']}, self.movie, upsert=True)
        except Exception as e:
            self.logger.error(e)
            pass

        try:
            self.db[config['DB']['COMMENT_QUEUE']].update_one({}, {'$push': {'movies': {'$each': self.movie['_id']}}})
        except Exception as e:
            self.logger.error(e)

        for maker in self.makers:
            try:
                self.db[config["DB"]["MAKERS"]].replace_one({'_id': maker['_id']}, maker, upsert=True)
            except pymongo.errors.DuplicateKeyError as dke:
                pass
            except Exception as e:
                self.logger.error(e)
            finally:
                pass
    
    def add_movie_ids(self):
        path = config["SCRAPER"]["NAVER_MOVIE_BASIC_PATH"] + self.current_target
        html = self._get_html(path)
        soup = BeautifulSoup(html, 'html.parser')

        try:
            tags = soup.find('ul', {'class': 'thumb_link_mv'}).find_all('a', {'class': 'thumb'})
            movie_ids = [tag['href'][tag['href'].index('=')+1:] for tag in tags]

            # add to queue
            db_ids = [doc['_id'] for doc in self.db[config["DB"]["MOVIES"]].find({'_id': {'$in': movie_ids}})]
            new_ids = list(set(movie_ids) - set(db_ids) - set(self.queue.queue))
            for id in new_ids:
                self.queue.put(id)
        except Exception as e:
            self.logger.error(e)
            new_ids = []
            pass

        # add to db
        try:
            if len(new_ids) > 0:
                self.db[config["DB"]["MOVIE_QUEUE"]].update_one({}, {'$push': {'movies': {'$each': new_ids}}})
        except Exception as e:
            self.logger.error(e)

    def refresh_status(self):
        try:
            self.db[config["DB"]["MOVIE_QUEUE"]].update_one({}, {'$pop': {'movies': -1}})
        except Exception as e:
            self.logger.error(e)
            pass

        self.movie = dict()
        self.makers = []
    
    def close_connection(self):
        self.client.close()

    def _set_queue(self):
        q = queue.Queue()
        movie_queue = self.db[config["DB"]["MOVIE_QUEUE"]]
        if movie_queue.count_documents({}) == 0 or len(movie_queue.find_one()['movies']) == 0:
            movie_ids = self._set_init_movie_list()
        else:
            movie_ids = movie_queue.find_one()['movies']
        
        for id in movie_ids:
            q.put(id)
        return q

    def _get_html(self, path):
        self.headers['path'] = path
        try:
            html = self.session.get(path, headers=self.headers, timeout=30).text
        except HTTPError as e:
            self.logger.error(e)
        except URLError as e:
            self.logger.error(e)
        return html

    def _set_init_movie_list(self):
        movie_ids = []

        # db comment collection에서 수집
        movies = self.db[config['DB']['MOVIES']]
        info_movie_ids = pd.DataFrame(movies.find({}, {'_id': 1}))

        reviews = self.db[config['DB']['USER_REVIEWS']]
        movie_ids_from_reviews = pd.DataFrame(reviews.find({}, {'_id': 0, 'movie_id': 1})).drop_duplicates()
        
        target_ids = movie_ids_from_reviews[~movie_ids_from_reviews['movie_id'].isin(info_movie_ids['_id'])]['movie_id'].to_list()
        movie_ids += target_ids

        # 랭킹 페이지에서 수집
        day = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        print(day)
        root_path = config["SCRAPER"]["NAVER_MOVIE_RANKING_PATH"]
        queries = ['cnt', 'cur', 'pnt']

        for query in queries:
            path = f'{root_path}?sel={query}&date={day}'
            print(path)
            html = self._get_html(path)

            soup = BeautifulSoup(html, 'html.parser')
            try:
                titles = soup.find('table', {'class': 'list_ranking'}).find_all('td', {'class': 'title'})
                tags = [title.find('a') for title in titles]
                urls = [tag['href'] for tag in tags]
            except Exception as e:
                self.logger.error(e)

            for url in urls:
                movie_id = url[url.index('=')+1:]
                if self.db[config["DB"]["MOVIES"]].find_one({ '_id': movie_id}) is None:
                    movie_ids.append(movie_id)
            sleep()
        
        movie_ids = list(set(movie_ids))

        try:
            self.db[config["DB"]["MOVIE_QUEUE"]].drop()
            self.db[config["DB"]["MOVIE_QUEUE"]].insert_one({'_id': 1, 'movies': movie_ids})
        except Exception as e:
            self.logger.error(e)

        self.logger.info(f'{len(movie_ids)} movies are added to queue.')
        print(f'{len(movie_ids)} movies are added to queue.')

        return movie_ids

    def _upload_to_s3(self, url_from, path_to):
        trial = 0
        while True:
            try:
                with requests.get(url_from, stream=True) as r:
                    self.s3.upload_fileobj(
                        r.raw, 
                        config['AWS']['S3_BUCKET'], 
                        path_to
                    )
                break
            except Exception as e:
                trial += 1
                self.logger.error(f'[trial {trial}]{e}')
                if trial > 9:
                    break
                time.sleep(1)
                continue

def sleep():
    return time.sleep(config["SCRAPER"]["MOVIE_INFO_SLEEP_INTERVAL"] - random.random()*0.2)

def main():
    scraper = MovieScraper()
    movie_count = 0
    maker_count = 0

    while True:
        if datetime.now() > stop_time:
            logger.info('time over')
            break

        if scraper.queue.qsize() < 1:
            logger.info('queue is empty.')
            break

        scraper.set_movie_id()
        scraper.scrape_basic()
        sleep()
        scraper.scrape_detail()
        scraper.upsert_data_to_db()
        movie_count += 1
        maker_count += len(scraper.get_makers())
        scraper.add_movie_ids()
        scraper.refresh_status()
        sleep()
    
    scraper.close_connection()
    return movie_count, maker_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('minutes', type=int, default=1, help='process duration (minutes)')
    args = parser.parse_args()

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'./logs/movie_info_scraper_{datetime.now().date()}.log', 
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    stop_time = datetime.now() + timedelta(minutes=+args.minutes)
    logger.info(f'process started. finish at {stop_time}')
    print(f'process started. finish at {stop_time}')

    start_time = time.time()

    # main
    movie_count, maker_count = main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. {movie_count} movies and {maker_count} reviews are processed for {duration}.')
    print(f'Finishing process. {movie_count} movies and {maker_count} makers are processed for {duration}.')