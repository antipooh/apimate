import pytest
from bson import ObjectId

from apimate.mongodb.query import MongodbSearchQuery
from apimate.query import FilterField, IntFilterField


class FakeSearch(MongodbSearchQuery):
    atext = FilterField()
    btext = FilterField()
    ivalue = IntFilterField()


@pytest.fixture
def make_query():
    def fabric(filter, offset=None):
        return FakeSearch(filter=filter, offset=offset)

    return fabric


def test_filter_ids(make_query):
    query = make_query({"ids": ['6086ae5ea8f76b5d464350f6', '6086ae5ea8f76b5d464350f8']})
    assert set(query.find['_id']['$in']) == {ObjectId('6086ae5ea8f76b5d464350f6'), ObjectId('6086ae5ea8f76b5d464350f8')}


@pytest.mark.parametrize('filter, result', (
        ({'atext': 'foo'}, {'atext': {'$eq': 'foo'}}),
        ({'atext': 'foo', 'btext': 'bar'}, {'atext': {'$eq': 'foo'}, 'btext': {'$eq': 'bar'}}),
        ({'ivalue': 12}, {'ivalue': {'$eq': 12}}),
))
def test_filters(filter, result, make_query):
    query = make_query(filter)
    assert query.find == result


def test_offset(make_query):
    query = make_query({}, offset='6086ae5ea8f76b5d464350f6')
    assert query.find == {'_id': {'$gt': ObjectId('6086ae5ea8f76b5d464350f6')}}
