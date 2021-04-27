import json

from flask import Flask
import boto3
import botocore

from model import RecDAO
from service import RecService
from view import view


def create_app():
    app = Flask(__name__)

    with open('../config.json') as f:
        config = json.load(f)

    # persistence layer
    s3 = boto3.client(
        's3',
        aws_access_key_id=config['AWS']['AWS_ACCESS_KEY'],
        aws_secret_access_key=config['AWS']['AWS_SECRET_KEY']
    )

    rec_dao = RecDAO(s3)

    # business layer
    rec_service = RecService(rec_dao)

    view.create_endpoints(app, rec_service)

    return app
