from typing import Optional

import pytest
from bson import ObjectId

from apimate.mongodb.crud import get_model, insert_model, remove_model, search_model, search_model_one, update_model, \
    update_model_one
from apimate.mongodb.types import InDBModel


class FakeModel(InDBModel):
    name: str
    value: Optional[float]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_insert_model(collection):
    saved = await insert_model(collection, lambda _: FakeModel, FakeModel(name='MODEL', value=15))
    assert isinstance(saved, FakeModel)
    assert saved.id is not None
    assert saved.name == 'MODEL'
    assert saved.value == 15


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_get_model(collection):
    loaded = await get_model(collection, lambda _: FakeModel, '60237341235cab81fd147e1a')
    assert isinstance(loaded, FakeModel)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_update_model_one(collection):
    assert await update_model_one(collection, '60237341235cab81fd147e1a', FakeModel(name='NEW-NAME')) is True
    obj = await collection.find_one({'_id': ObjectId('60237341235cab81fd147e1a')})
    assert obj['name'] == 'NEW-NAME'
    assert obj['value'] == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_update_model(collection):
    assert await update_model(collection, {'value': {'$lt': 20}}, {'$set': {'name': 'UPDATED'}}) is True
    ls = [it['_id'] async for it in collection.find({'name': 'UPDATED'})]
    assert ls == [ObjectId('60237341235cab81fd147e1b')]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model(collection):
    ls = [it async for it in search_model(collection, lambda _: FakeModel, {'value': {'$lt': 10}})]
    assert ls == [FakeModel(id='60237341235cab81fd147e1b', name='MODEL-3', value=0.02)]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model_one(collection):
    loaded = await search_model_one(collection, lambda _: FakeModel, {'name': 'MODEL-2'})
    assert isinstance(loaded, FakeModel)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_remove_model(collection):
    assert await remove_model(collection, '60237341235cab81fd147e1b') is True
    ls = [it['_id'] async for it in collection.find()]
    assert set(ls) == {ObjectId('60237341235cab81fd147e19'), ObjectId('60237341235cab81fd147e1a')}
