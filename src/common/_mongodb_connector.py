import json

from pymongo import MongoClient

with open('../config.json') as f:
    config = json.load(f)


class MongoConnector(object):
    def __init__(self):
        self.__client = MongoClient(config['DB']['DB_URL'], config['DB']['DB_PORT'])
        self.__db = self.client[config['DB']['DATABASE']]
        self.__test_db = self.client[config['DB']['TEST_DATABASE']]
        self.__test_col = self.db[config['DB']['TEST_COLLECTION']]
        self.__movies = self.db[config['DB']['MOVIES']]
        self.__makers = self.db[config['DB']['MAKERS']]
        self.__movie_queue = self.db[config['DB']['MOVIE_QUEUE']]
        self.__comment_queue = self.db[config['DB']['COMMENT_QUEUE']]
        self.__user_reviews = self.db[config['DB']['USER_REVIEWS']]
        self.__user_review_tokens = self.db[config['DB']['USER_REVIEW_TOKENS']]
        self.__user_review_tokens_okt = self.db[config['DB']['USER_REVIEW_TOKENS_OKT']]
        self.__user_review_morphs = self.db[config['DB']['USER_REVIEW_MORPHS']]
        self.__user_review_morphs_okt = self.db[config['DB']['USER_REVIEW_MORPHS_OKT']]
        self.__okt_adjective_stat = self.db[config['DB']['OKT_ADJECTIVE_STAT']]
 
    
    @property
    def client(self):
        return self.__client
    
    @property
    def db(self):
        return self.__db

    @property
    def test_db(self):
        return self.__test_db
    
    @property
    def test_col(self):
        return self.__test_col
    
    @property
    def movies(self):
        return self.__movies
    
    @property
    def makers(self):
        return self.__makers
    
    @property
    def movie_queue(self):
        return self.__movie_queue
    
    @property
    def comment_queue(self):
        return self.__comment_queue
    
    @property
    def user_reviews(self):
        return self.__user_reviews
    
    @property
    def user_review_tokens(self):
        return self.__user_review_tokens
    
    @property
    def user_review_tokens_okt(self):
        return self.__user_review_tokens_okt
    
    @property
    def user_review_morphs(self):
        return self.__user_review_morphs
    
    @property
    def user_review_morphs_okt(self):
        return self.__user_review_morphs_okt
    
    @property
    def okt_adjective_stat(self):
        return self.__okt_adjective_stat


    def close(self):
        self.__client.close()