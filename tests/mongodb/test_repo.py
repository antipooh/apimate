from typing import Optional

import pytest
from bson import ObjectId

from apimate.mongodb.repo import MongoRepo
from apimate.mongodb.types import InDBModel


class FakeModel(InDBModel):
    name: str
    value: Optional[float]


class FakeRepo(MongoRepo):
    collection_name = 'fake'
    model_type = FakeModel


@pytest.fixture
def repo(db):
    return FakeRepo(db)


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_get_collection(repo, collection):
    assert await repo.get_collection() == collection


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_insert_model(repo):
    saved = await repo._insert_model(FakeModel(name='MODEL', value=15))
    assert isinstance(saved, FakeModel)
    assert saved.id is not None
    assert saved.name == 'MODEL'
    assert saved.value == 15


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_get_model(repo):
    loaded = await repo._get_model('60237341235cab81fd147e1a')
    assert isinstance(loaded, FakeModel)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_update_model_one(repo, collection):
    assert await repo._update_model_one('60237341235cab81fd147e1a', FakeModel(name='NEW-NAME')) is True
    obj = await collection.find_one({'_id': ObjectId('60237341235cab81fd147e1a')})
    assert obj['name'] == 'NEW-NAME'
    assert obj['value'] == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_update_model(repo, collection):
    assert await repo._update_model({'value': {'$lt': 20}}, {'$set': {'name': 'UPDATED'}}) is True
    ls = [it['_id'] async for it in collection.find({'name': 'UPDATED'})]
    assert ls == [ObjectId('60237341235cab81fd147e1b')]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model(repo):
    ls = [it async for it in await repo._search_model({'value': {'$lt': 10}})]
    assert ls == [FakeModel(id='60237341235cab81fd147e1b', name='MODEL-3', value=0.02)]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model_one(repo):
    loaded = await repo._search_model_one({'name': 'MODEL-2'})
    assert isinstance(loaded, FakeModel)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_remove_model(repo, collection):
    assert await repo._remove_model('60237341235cab81fd147e1b') is True
    ls = [it['_id'] async for it in collection.find()]
    assert set(ls) == {ObjectId('60237341235cab81fd147e19'), ObjectId('60237341235cab81fd147e1a')}


