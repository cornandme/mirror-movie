import json

import boto3
import botocore
from flask import Flask
from flask_cors import CORS

with open('../config.json') as f:
    config = json.load(f)

from model import MovieInfoDAO
from model import RecDAO
from model import WordModel
from service import MovieInfoService
from service import RecService
from service import SearchService
from view import view


def create_app():
    app = Flask(__name__)
    CORS(app)

    # persistence layer
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
    )

    movie_info_dao = MovieInfoDAO(s3, config)
    print('movie_info_dao loaded')
    rec_dao = RecDAO(s3, config)
    print('rec_dao loaded')
    word_model = WordModel(s3, config)
    print('word_model loaded')

    # business layer
    movie_info_service = MovieInfoService(movie_info_dao)
    rec_service = RecService(rec_dao)
    search_service = SearchService(word_model, rec_dao)

    # presentation layer
    view.create_endpoints(app, movie_info_service, rec_service, search_service)

    return app