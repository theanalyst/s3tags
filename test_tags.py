#!/usr/bin/env python
import tags
import pytest

@pytest.fixture()
def s3client():
    s3 = tags.S3("access","secret","http://localhost:8000","tagbucket",True)
    assert s3.get_or_create_bucket()
    yield s3
    s3.cleanup()

def test_create_simple_object(s3client):
    r = s3client.put_object('foo','foobar')
    assert r is not None
    #assert r['ResponseMetadata']['HTTPStatusCode'] == 200
    r = s3client.get_tags('foo')
    assert len(r['TagSet']) == 0

def test_put_obj_simple_tag(s3client):
    s3client.put_object('foo1','foobar','key=val')
    r = s3client.get_tags('foo1')
    assert len(r['TagSet']) == 1  # Trivial, we have a non empty tag set
    tagset = r['TagSet'][0]
    assert len(tagset) == 2
    assert r['TagSet'][0]['Key'] == 'key'

def test_put_obj_simple_tag2(s3client):
    s3client.put_object('foo2','foobar','foo=bar&baz')
    r = s3client.get_tags('foo2')
    assert len(r['TagSet']) == 2  # Trivial, we have a non empty tag set
    #tagset = r['TagSet'][0]
    #assert len(tagset) == 2
    #assert r['TagSet'][0]['Key'] == 'key'
