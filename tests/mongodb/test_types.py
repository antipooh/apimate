from datetime import time, timedelta
from decimal import Decimal
from enum import Enum

import pytest
from bson import ObjectId

from apimate.mongodb.types import from_mongo, to_mongo


class FakeEnum(Enum):
    ONE = 'one'
    TWO = 'two'


@pytest.mark.parametrize('value, result', (
        (FakeEnum.TWO, 'two'),
        (Decimal('-12.45'), '-12.45'),
        (timedelta(hours=1, minutes=10, seconds=13, microseconds=12), 4213.000012),
        ({1, 3, 5, 7}, [1, 3, 5, 7]),
        (time(hour=12, minute=3), '12:03:00')
))
@pytest.mark.mongodb
def test_to_mongo(value, result):
    assert to_mongo({'value': value}) == {'value': result}


@pytest.mark.mongodb
def test_from_mongo():
    assert from_mongo({'_id': ObjectId('5f841314a3dbce365e02bfa1')}) == {'id': ObjectId('5f841314a3dbce365e02bfa1')}
