#!/usr/bin/env python
import argparse
import logging
import string
import random

from functools import wraps, partial

import boto3
import botocore
from botocore.client import Config


def make_random_string(size):
    return ''.join(random.choice(string.ascii_letters) for _ in range(size))


def handle_error(fatal=False):
    def make_req(req):
        @wraps(req)
        def call_req(*args,**kwargs):
            try:
                return req(*args,**kwargs)
            except botocore.exceptions.ClientError as e:
                logger = logging.getLogger(__name__)
                logger.error(e)
                if fatal:
                    logger.error('Fatal error, cannot continue after %s failure, exiting' % req.__name__)
        return call_req
    return make_req

class S3():
    def __init__(self, access, secret, endpoint, bucket, debug=True, region='us-east-1'):
        self.s3 = boto3.client(
            's3',region,
            endpoint_url = endpoint,
            aws_access_key_id = access,
            aws_secret_access_key = secret,
        )
        self.bucket = bucket
        self.obj_set = set()
        self.logger = logging.getLogger(__name__)
        if debug:
            boto3.set_stream_logger(name='botocore',level=logging.DEBUG)
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    @staticmethod
    def make_tags(tags):
        tag_list = []
        for k,v in tags.items():
            tag_list.append({'Key':k,
                             'Value':v if v is not None else ''})

        return {'TagSet':tag_list}

    @handle_error(fatal=True)
    def _create_bucket(self):
        return self.s3.create_bucket(Bucket=self.bucket)

    @handle_error()
    def get_or_create_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise  # our logger will catch this!
            return self._create_bucket()
        return True

    @handle_error(fatal=False)
    def put_object(self,obj,body='abcd',tags=None):
        s3_put_obj = partial(self.s3.put_object,Bucket=self.bucket, Key=obj, Body=body)

        if tags is not None:
            return s3_put_obj(Tagging=tags)
        else:
            return s3_put_obj()

    @handle_error(fatal=False)
    def get_tags(self, obj):
        return self.s3.get_object_tagging(Bucket=self.bucket, Key=obj)

    @handle_error(fatal=False)
    def put_tags(self,obj,tags):
        return self.s3.put_object_tagging(Bucket=self.bucket, Key=obj)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='test s3 tags')
    parser.add_argument('--access', type=str, default='access',
                        help='AWS access key id')
    parser.add_argument('--secret', type=str, default='secret',
                        help='AWS Secret Access Key')
    parser.add_argument('--endpoint', type=str, default='http://localhost:8000',
                        help='S3 host')
    parser.add_argument('--bucket', type=str, default = 'bucket1',
                        help='bucket name')
    parser.add_argument('--cleanup', type=bool, default = False,
                        help='delete bucket and objects after test (default: False)')
    parser.add_argument('--debug', type=bool, default=False,
                        help='set boto debug')
    args = parser.parse_args()
    s3test = S3(args.access, args.secret, args.endpoint, args.bucket, args.debug)
    s3test.get_or_create_bucket()
    s3test.put_object('foo','abcdefg')
    s3test.put_object('foobar','abcde','')
