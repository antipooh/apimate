import pytest
from bson import ObjectId

from apimate.mongodb.query import MongodbSearchQuery, Relation, inject_relations
from apimate.query import IntQueryField, QueryField


class FakeSearch(MongodbSearchQuery):
    atext = QueryField()
    btext = QueryField()
    ivalue = IntQueryField()


@pytest.fixture
def make_query():
    def fabric(filter, sort=None):
        return FakeSearch(filter=filter, sort=sort)

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


class TestRelation:

    @pytest.fixture(autouse=True)
    def set_environment(self, mocker):
        self.search = mocker.AsyncMock()
        self.relation = Relation('relation_id', 'relation', self.search)

    def test_extract_id(self, mocker):
        self.relation.extract_id(mocker.Mock(relation_id='RELATION-ID'))
        assert self.relation.ids == {'RELATION-ID'}

    @pytest.mark.asyncio
    async def test_load(self):
        self.relation.ids = {'608f2af5717eb800f2d41a5a'}
        await self.relation.load()
        self.search.assert_awaited_with({'_id': {'$in': [ObjectId('608f2af5717eb800f2d41a5a')]}})

    def test_inject(self, mocker):
        item = mocker.Mock(relation_id='RELATION-ID')
        self.relation.models['RELATION-ID'] = 42
        assert self.relation.inject(item) == item
        assert item.relation == 42


@pytest.mark.asyncio
async def test_inject_relations(mocker):
    items = mocker.Mock(items=[mocker.Mock(relation_id='608f2af5717eb800f2d41a5a')])
    found = mocker.MagicMock()
    related = mocker.Mock(id='608f2af5717eb800f2d41a5a')
    found.__aiter__.return_value = [related]
    search = mocker.AsyncMock(return_value=found)
    await inject_relations(items, [Relation('relation_id', 'relation', search)])
    search.assert_awaited()
    assert items.items[0].relation == related
