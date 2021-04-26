from typing import Type

import pytest

from apimate.mongodb.crud import SavedModel
from apimate.mongodb.repo import MongoRepo
from apimate.mongodb.types import InDBModel


class FakeMultiModel1(InDBModel):
    name: str


class FakeMultiModel2(FakeMultiModel1):
    value: float


class FakeMultiRepo(MongoRepo):
    collection_name = 'fake'

    def model_type_select(self, doc: dict) -> Type[SavedModel]:
        return FakeMultiModel2 if isinstance(doc.get('value'), (float, int)) else FakeMultiModel1


@pytest.fixture
def repo(db):
    return FakeMultiRepo(db)


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_insert_model_1(repo):
    saved = await repo._insert_model(FakeMultiModel1(name='MODEL'))
    assert isinstance(saved, FakeMultiModel1)
    assert saved.id is not None
    assert saved.name == 'MODEL'


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_insert_model_1(repo):
    saved = await repo._insert_model(FakeMultiModel2(name='MODEL', value=15))
    assert isinstance(saved, FakeMultiModel2)
    assert saved.id is not None
    assert saved.name == 'MODEL'
    assert saved.value == 15


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_get_model_1(repo):
    loaded = await repo._get_model('60237341235cab81fd147e19')
    assert isinstance(loaded, FakeMultiModel1)
    assert loaded.id == '60237341235cab81fd147e19'
    assert loaded.name == 'MODEL-1'


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_get_model_2(repo):
    loaded = await repo._get_model('60237341235cab81fd147e1a')
    assert isinstance(loaded, FakeMultiModel2)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model(repo):
    ls = [it async for it in await repo._search_model({'name': {'$ne': 'MODEL-2'}})]
    assert ls == [
        FakeMultiModel1(id='60237341235cab81fd147e19', name='MODEL-1'),
        FakeMultiModel2(id='60237341235cab81fd147e1b', name='MODEL-3', value=0.02),
    ]


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model_one_1(repo):
    loaded = await repo._search_model_one({'value': {'$eq': None}})
    assert isinstance(loaded, FakeMultiModel1)
    assert loaded.id == '60237341235cab81fd147e19'
    assert loaded.name == 'MODEL-1'


@pytest.mark.asyncio
@pytest.mark.mongodb
async def test_search_model_one_2(repo):
    loaded = await repo._search_model_one({'value': {'$gt': 10}})
    assert isinstance(loaded, FakeMultiModel2)
    assert loaded.id == '60237341235cab81fd147e1a'
    assert loaded.name == 'MODEL-2'
    assert loaded.value == 33.01
