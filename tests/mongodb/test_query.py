import pytest
from bson import ObjectId

from apimate.mongodb.query import MongodbSearchBuilder, Relation, inject_list_relations, inject_relations
from apimate.query import IntQueryField, SearchQuery, TextQueryField


class FakeSearch(SearchQuery):
    atext = TextQueryField()
    btext = TextQueryField()
    ivalue = IntQueryField()


@pytest.fixture
def make_builder():
    def fabric(filter, sort=None):
        return MongodbSearchBuilder(FakeSearch(filter=filter, sort=sort))

    return fabric


def test_filter_ids(make_builder):
    query = make_builder({"ids": ['6086ae5ea8f76b5d464350f6', '6086ae5ea8f76b5d464350f8']})
    assert set(query.filter['_id']['$in']) == {ObjectId('6086ae5ea8f76b5d464350f6'), ObjectId('6086ae5ea8f76b5d464350f8')}


@pytest.mark.parametrize('filter, result', (
        ({'atext': 'foo'}, {'atext': {'$eq': 'foo'}}),
        ({'atext': 'foo', 'btext': 'bar'}, {'atext': {'$eq': 'foo'}, 'btext': {'$eq': 'bar'}}),
        ({'ivalue': 12}, {'ivalue': {'$eq': 12}}),
))
def test_filters(filter, result, make_builder):
    query = make_builder(filter)
    assert query.filter == result


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


@pytest.fixture
def search(mocker):
    def fabric(id, value):
        found = mocker.MagicMock()
        related = mocker.Mock(id=id, value=value)
        found.__aiter__.return_value = [related]
        return mocker.AsyncMock(return_value=found)

    return fabric


@pytest.mark.asyncio
async def test_inject_relations(mocker, search):
    model = mocker.Mock(relation_id='608f2af5717eb800f2d41a5a', other_id='608f2af5717eb800f2d41a5b')
    search_1 = search('608f2af5717eb800f2d41a5a', 11)
    search_2 = search('608f2af5717eb800f2d41a5b', 12)
    await inject_relations(model, [
        Relation('relation_id', 'relation', search_1),
        Relation('other_id', 'other', search_2)
    ])
    search_1.assert_awaited()
    search_2.assert_awaited()
    assert model.relation.value == 11
    assert model.other.value == 12


@pytest.mark.asyncio
async def test_inject_list_relations(mocker, search):
    items = mocker.Mock(items=[mocker.Mock(relation_id='608f2af5717eb800f2d41a5a', other_id=None),
                               mocker.Mock(relation_id='608f2af5717eb800f2d41a5a', other_id='608f2af5717eb800f2d41a5b')
                               ])
    search_1 = search('608f2af5717eb800f2d41a5a', 11)
    search_2 = search('608f2af5717eb800f2d41a5b', 12)
    await inject_list_relations(items, [
        Relation('relation_id', 'relation', search_1),
        Relation('other_id', 'other', search_2)
    ])
    search_1.assert_awaited()
    search_2.assert_awaited()
    assert items.items[0].relation.value == 11
    assert items.items[0].other is None
    assert items.items[1].relation.value == 11
    assert items.items[1].other.value == 12
