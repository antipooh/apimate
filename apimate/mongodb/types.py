from datetime import time, timedelta, datetime
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, Union, List

from bson import ObjectId
from bson.errors import InvalidId
from pydantic import BaseModel, PydanticValueError


class RangeBorderCrossing(PydanticValueError):
    code = 'range.border_crossing'
    msg_template = 'Make sure the borders do not cross'


class DatetimeBorderCrossing(RangeBorderCrossing):
    code = 'datetime_range.border_crossing'


class ObjectIdStr(str):
    """Field for validate string like ObjectId"""

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if isinstance(v, ObjectId):
            return str(v)
        else:
            try:
                ObjectId(str(v))
            except InvalidId:
                raise ValueError('Not a valid ObjectId')
            return v


def to_mongo(data: Dict) -> Dict:
    if isinstance(data, dict):
        _data = {}
        for k, v in data.items():
            if isinstance(v, Enum):
                v = v.value
            elif isinstance(v, Decimal):
                v = str(v)
            elif isinstance(v, timedelta):
                v = v.total_seconds()
            elif isinstance(v, time):
                v = v.isoformat()
            elif isinstance(v, dict):
                v = to_mongo(v)
            elif isinstance(v, (list, set, frozenset)):
                v = [to_mongo(it) for it in v]
            _data.update({k: v})
        _data.pop('id', None)
        return _data
    else:
        return data


def from_mongo(data: Dict) -> Dict:
    object_id = data.pop('_id', None)
    if object_id:
        data['id'] = object_id
    return data


class InDBModel(BaseModel):
    id: Optional[ObjectIdStr]


class DateTimeRange(BaseModel):
    """
    Datetime range type.

    First elem - lower bound.
    Second elem - upper bound.
    """

    gte: datetime = None
    lte: datetime = None

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: Union[List, Dict], values=None):
        """Check border crossing"""
        gte, lte = None, None
        try:
            if isinstance(v, list):
                gte, lte = v
            elif isinstance(v, dict):
                gte = v.get('gte')
                lte = v.get('lte')
        except IndexError:
            return cls(gte=gte, lte=lte)

        if gte and lte:
            if gte > lte or lte < gte:
                raise DatetimeBorderCrossing()

        return cls(gte=gte, lte=lte)
