import argparse
from datetime import datetime
from datetime import timedelta
import json
import logging
import os
from pathlib import Path
import math
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


class UserReviewScraper:
    def __init__(self):
        self.logger = logging.getLogger()
        self.client = MongoClient(config["DB"]["DB_URL"], config["DB"]["DB_PORT"])
        self.db = self.client[config["DB"]["DATABASE"]]
        self.session = requests.Session()
        self.headers = config["SCRAPER"]["HEADERS"]
        self.update_checker = self._set_update_checker()
        self.queue = self._set_queue()
        self.current_target = None
        self.last_update_date = None
        self.pages = 0
        self.current_page = 1
        self.review_count = 0
        self.reviews = []
        self.valid = True

    def get_review_count(self):
        return self.review_count   

    def get_qsize(self):
        return self.queue.qsize()

    def get_current_target(self):
        return self.current_target

    def get_current_page(self):
        return self.current_page
    
    def get_pages(self):
        return self.pages
    
    def get_last_update_date(self):
        return self.last_update_date

    def get_review_count(self):
        return self.review_count

    def get_valid(self):
        return self.valid

    def increase_current_page(self):
        self.current_page += 1
        
    def set_target(self):
        self.current_target = self.queue.get()
        self.last_update_date = self.update_checker.loc[self.current_target, 'date'] \
                                if self.current_target in self.update_checker.index else None


    def check_validation(self):
        path = f"{config['SCRAPER']['NAVER_MOVIE_REVIEW_PATH_PREFIX']}{self.current_target}{config['SCRAPER']['NAVER_MOVIE_REVIEW_PATH_QUERY']}"
        html = self._get_html(path)
        soup = BeautifulSoup(html, 'html.parser')

        # no rate
        try:
            total_tag = soup.find('strong', {'class': 'total'})
            if total_tag is None or total_tag.find('em').get_text(strip=True) == '0':
                self.valid = False
                return
        except Exception as e:
            self.logger.error(e)
            self.valid = False
            return

        # no new rate
        try:
            date = soup.find('div', {'class': 'score_result'}).find('li').find('div', {'class': 'score_reple'}).find('dt').find_all('em')[-1] \
                    .get_text(strip=True).replace('.', '').replace(' ', '').replace(':', '')
            if self.last_update_date is not None and date < self.last_update_date:
                print('no new rate')
                self.valid = False
                return
        except Exception as e:
            self.logger.error(e)
            self.valid = False
            return
        
        try:
            rate_count = int(soup.find('strong', {'class': 'total'}).find('em').get_text(strip=True).replace(',', ''))
            self.pages = math.ceil(rate_count / 10)
        except Exception as e:
            self.logger.error(e)
            self.valid = False
            return

        return soup

    def scrape_review_page(self, soup=None):
        if self.current_page > 1:
            path = f"{config['SCRAPER']['NAVER_MOVIE_REVIEW_PATH_PREFIX']}{self.current_target}{config['SCRAPER']['NAVER_MOVIE_REVIEW_PATH_QUERY']}&page={self.current_page}"
            html = self._get_html(path)
            soup = BeautifulSoup(html, 'html.parser')

        self._scrape_reviews(soup)

    def insert_reviews_to_db(self):
        for document in self.reviews:
            try:
                self.db[config['DB']['USER_REVIEWS']].insert_one(document)
                self.review_count += 1
            except pymongo.errors.DuplicateKeyError as dke:
                self.logger.warning(dke, document)
            except Exception as e:
                self.logger.error(e)
            finally:
                pass

    def reset_reviews(self):
        self.reviews = []
        self.review_count = 0
    
    def reset_status(self):
        self.last_update_date = None
        self.current_page = 1
        self.pages = 0
        self.review_count = 0
        self.reviews = []
        self.valid = True

    def close_connection(self):
        self.client.close()

    def _set_queue(self):
        q = queue.Queue()

        # db 큐 목록 넘겨받기
        comment_queue = self.db[config['DB']['COMMENT_QUEUE']]
        movie_ids = []
        if comment_queue.count_documents({}) == 0 or len(comment_queue.find_one()['movies']) == 0:
            pass
        else:
            try:
                movie_ids = [movie_id for movie_id in comment_queue.find_one()['movies']]
                comment_queue.update_one({'_id': 1}, {'$set': {'movies': []}})
            except Exception as e:
                self.logger.error(e)
        print(f'{len(movie_ids)} from queue')

        # db movies collection에서 cold movies 수집
        now = datetime.now()
        target_datetime = now - timedelta(days=30)
        movies = self.db[config['DB']['MOVIES']]

        info_movie_ids = pd.DataFrame(movies.find(
            {'review_checked_date': {'$not': {'$gte': target_datetime}}}, {'_id': 1, 'review_checked_date': 1}
        )[:1000])
        for movie_id in info_movie_ids['_id']:
            movies.update_one({'_id': movie_id}, {'$set': {'review_checked_date': now}})

        reviews = self.db[config['DB']['USER_REVIEWS']]
        movie_ids_from_reviews = pd.DataFrame(reviews.find({}, {'_id': 0, 'movie_id': 1})).drop_duplicates()

        target_ids = info_movie_ids[
            ~info_movie_ids['_id'].isin(movie_ids_from_reviews['movie_id'])
        ]['_id'].to_list()
        
        print(f'{len(target_ids)} from movie_info')
        movie_ids += target_ids

        # 신규 코멘트 목록에서 영화 리스트 받아오기
        path_root = config['SCRAPER']['NAVER_MOVIE_COMMENT_ROOT_PATH']
        page = 1
        last_date = self.update_checker['date'].max()[2:-4]

        new_movie_ids = []
        while True:
            # 정지조건
            if page > 1000:
                break

            path = f'{path_root}?&page={page}'
            html = self.session.get(path, headers=self.headers).text
            soup = BeautifulSoup(html, 'html.parser')
            
            # 날짜 확인
            comment_date = soup.find('table', {'class': 'list_netizen'}) \
                .find('tbody').find('tr').find(lambda tag: tag.name == 'td' and tag.get('class') == ['num']) \
                .find('br').next_sibling \
                .replace('.', '')
            if comment_date < last_date:
                break

            # 영화 id 수집
            tags = soup.find('table', {'class': 'list_netizen'}).find('tbody').find_all('td', {'class': 'title'})
            for tag in tags:
                movie_id = tag.find('a', {'class': 'movie color_b'})['href'] \
                    .replace('?st=mcode&sword=', '').replace('&target=after', '')
                new_movie_ids.append(movie_id)
            
            # 다음 페이지
            page += 1
            sleep()
        print(f'{len(new_movie_ids)} new movie ids from new comments')
        
        # 큐에 id 넣기
        for id in set(movie_ids + new_movie_ids):
            q.put(id)
        
        return q


    def _set_update_checker(self):
        reviews = self.db[config['DB']['USER_REVIEWS']]
        update_checker = pd.DataFrame(reviews.find({}, {'_id': 0, 'movie_id': 1, 'date': 1})) \
                            .groupby('movie_id').max()
        return update_checker


    def _get_html(self, path):
        self.headers['path'] = path
        try:
            html = self.session.get(path, headers=self.headers, timeout=30).text
        except HTTPError as e:
            self.logger.error(e)
        except URLError as e:
            self.logger.error(e)
        return html

    def _scrape_reviews(self, soup):
        tags = soup.find('div', {'class': 'score_result'}).find_all('li')
        for tag in tags:
            # date
            try:
                date = tag.find('div', {'class': 'score_reple'}).find('dt').find_all('em')[-1].get_text(strip=True).replace('.', '').replace(' ', '').replace(':', '')
                if self.last_update_date is not None and date < self.last_update_date:
                    self.valid = False
                    self.logger.info(f'scraped all new documents after {self.last_update_date}')
                    break
            except Exception as e:
                self.logger.error(e)
                continue

            # review
            try:
                review_tag = tag.find('div', {'class': 'score_reple'}).find('span', {'id': re.compile('_filtered_ment')})
                if review_tag.find('a') is not None:
                    review = review_tag.find('a', {'href': re.compile('javascript')})['data-src'].strip()
                else:
                    review = review_tag.get_text(strip=True)
            except Exception as e:
                self.logger.error(e)
                continue
            
            # certificated
            try:
                certificate_tag = tag.find('div', {'class': 'score_reple'}).find('span', {'class': 'ico_viewer'})
                certificated = True if certificate_tag != None and certificate_tag.get_text(strip=True) == '관람객' else False
            except Exception as e:
                self.logger.error(e)
                pass
            
            # rate
            try:
                rate = int(tag.find('div', {'class': 'star_score'}).find('em').get_text(strip=True))
            except Exception as e:
                self.logger.error(e)
                pass

            # user_id
            try:
                user_id_span = tag.find('div', {'class': 'score_reple'}).find('dt').find('span').get_text(strip=True)
                if '(' in user_id_span:
                    idx = user_id_span.index('(')
                    user_id = user_id_span[idx+1:idx+5]
                else:
                    user_id = user_id_span[:4]
            except Exception as e:
                self.logger.error(e)
                continue
            
            docu = {
                '_id': f'{user_id}_{date}',
                'movie_id': self.current_target,
                'rate': rate,
                'review': review,
                'certificated': certificated,
                'date': date,
                'tokenized': False
            }
            self.reviews.append(docu)

def sleep():
    return time.sleep(config['SCRAPER']['REVIEW_SLEEP_INTERVAL'] - random.random() * 0.2)

def main():
    movie_count = 0
    review_count = 0
    
    while True:
        if datetime.now() > stop_time:
            logger.info('time over')
            break

        if scraper.get_qsize() < 1:
            logger.info('queue is empty.')
            break

        scraper.set_target()
        logger.info(f'scraping movie id: {scraper.get_current_target()}, last update: {scraper.get_last_update_date()}')

        soup = scraper.check_validation()
        sleep()
        if not scraper.get_valid():
            scraper.reset_status()
            continue

        while True:
            if not scraper.get_valid():
                break
            if scraper.get_current_page() > scraper.get_pages():
                logger.info('all pages processed')
                break
            scraper.scrape_review_page(soup)
            scraper.insert_reviews_to_db()
            review_count += scraper.get_review_count()
            scraper.reset_reviews()
            scraper.increase_current_page()
            sleep()
            
        movie_count += 1
        scraper.reset_status()
    
    scraper.close_connection()
    return movie_count, review_count


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-root_path', type=str, default=None, help='config file path. use for airflow DAG.')
    parser.add_argument('-minutes', type=int, default=1440, help='process duration (minutes)')
    args = parser.parse_args()

    if args.root_path:
        os.chdir(f'{args.root_path}/scrapers')

    with open('../config.json') as f:
        config = json.load(f)

    logging.basicConfig(
        format='[%(asctime)s|%(levelname)s|%(module)s:%(lineno)s %(funcName)s] %(message)s', 
        filename=f'../logs/{Path(__file__).stem}_{datetime.now().date()}.log',
        level=logging.DEBUG
    )
    logger = logging.getLogger()
    stop_time = datetime.now() + timedelta(minutes=args.minutes)
    logger.info(f'process started. finish at {stop_time}')
    print(f'process started. finish at {stop_time}')
    
    start_time = time.time()
    scraper = UserReviewScraper()

    # main
    movie_count, review_count = main()

    duration = str(timedelta(seconds=(time.time() - start_time)))
    logger.info(f'Finishing process. {movie_count} movies and {review_count} reviews are processed for {duration}.')
    print(f'Finishing process. {movie_count} movies and {review_count} reviews are processed for {duration}.')