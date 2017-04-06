#!/usr/bin/env python
import tags
import pytest

@pytest.fixture()
def s3client():
    s3 = tags.S3("access","secret","http://localhost:8000","tagbucket",False)
    assert s3.get_or_create_bucket()
    yield s3
    s3.cleanup()

Body='abcdefg' # Contents of the obj

def response_to_dict(response):
    d = dict()
    for kv in response['TagSet']:
        key = kv['Key']
        val = None
        if 'Value' in kv:
            val = kv['Value']

        d[key] = val

    return d

def test_create_simple_object(s3client):
    r = s3client.put_object('foo',Body)
    assert r is not None
    assert r['ResponseMetadata']['HTTPStatusCode'] == 200
    r = s3client.get_tags('foo')
    assert len(r['TagSet']) == 0

def test_put_obj_simple_tagstr(s3client):
    s3client.put_object('foo1',Body,'key=val')
    r = s3client.get_tags('foo1')
    assert len(r['TagSet']) == 1  # Trivial, we have a non empty tag set
    d = response_to_dict(r)
    assert d == {'key':'val'}
    tagset = r['TagSet'][0]
    print (tagset)
    assert len(tagset) == 2
    assert r['TagSet'][0]['Key'] == 'key'

def test_put_obj_simple_tag(s3client):
    obj_name = 'obj_simple_tag'
    input_tags = {'fruit':'apple'}
    s3client.put_object(obj_name, Body, tagmap=input_tags)
    r = s3client.get_tags(obj_name)
    output_tags = response_to_dict(r)
    assert input_tags == output_tags


def test_put_obj_simple_tag2(s3client):
    s3client.put_object('foo2', Body, 'foo=bar&baz')
    r = s3client.get_tags('foo2')
    input_tags = {'foo':'bar','baz':None}
    output_tags = response_to_dict(r)
    assert input_tags == output_tags

def test_put_tags_api(s3client):
    input_tags = {'foo': 'bar'}
    obj_name = 'foo3'
    s3client.put_object(obj_name, Body)
    s3client.put_tags(obj_name, input_tags)
    r = s3client.get_tags(obj_name)
    output_tags = response_to_dict(r)
    assert input_tags == output_tags
