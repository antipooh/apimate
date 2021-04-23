import pytest
from motor.motor_asyncio import AsyncIOMotorClient


@pytest.fixture
def db(mongodb):
    client = AsyncIOMotorClient('{}:{}'.format(mongodb.client.HOST, mongodb.client.PORT))
    database = client[mongodb.name]
    yield database
    database.client.close()


@pytest.fixture
def collection(db):
    return db.fake
