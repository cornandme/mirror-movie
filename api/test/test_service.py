import pytest

import json
import numpy

import boto3

with open('../config.json') as f:
    config = json.load(f)

from model import RecDAO
from model import WordModel

from service import RecService
from service import SearchService

s3 = boto3.client(
    's3',
    aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
    aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
)

@pytest.fixture
def rec_service():
    return RecService(RecDAO(s3, config))

def test_get_newest_rec(rec_service):
    assert rec_service.get_newest_rec()

def test_get_actor_rec(rec_service):
    assert rec_service.get_actor_rec('159074')

def test_get_director_rec(rec_service):
    assert rec_service.get_director_rec('159074')

def test_get_similar_rec(rec_service):
    assert rec_service.get_similar_rec('159074')