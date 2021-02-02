from datetime import datetime
from typing import Dict, ForwardRef, List, Union

from pydantic import BaseModel, PydanticValueError

JsonScalar = Union[None, bool, int, float, str]
JsonData = Union[
    List[Union[JsonScalar, ForwardRef('JsonData')]],
    Dict[JsonScalar, Union[JsonScalar, ForwardRef('JsonData')]]
]

ID_STRING = r'[0-9a-fA-F]{24}'


class RangeBorderCrossing(PydanticValueError):
    code = 'range.border_crossing'
    msg_template = 'Make sure the borders do not cross'


class DatetimeBorderCrossing(RangeBorderCrossing):
    code = 'datetime_range.border_crossing'


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
