import pytest

import json
import numpy

import boto3

with open('../config.json') as f:
    config = json.load(f)

from model import RecDAO
from model import WordModel

s3 = boto3.client(
    's3',
    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
    aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
)

@pytest.fixture
def rec_dao():
    return RecDAO(s3, config)

@pytest.fixture
def word_model():
    return WordModel(s3, config)

def test_load_all_recs(rec_dao):
    assert rec_dao.newest_rec

def test_load_all_models(word_model):
    assert word_model.fasttext_word_model

def test_fasttext_word_model(word_model):
    assert type(word_model.fasttext_word_model.wv['이상해']) == numpy.ndarray