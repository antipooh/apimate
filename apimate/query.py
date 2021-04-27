import abc
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple, Type, TypeVar

from fastapi import Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Json, ValidationError, conint, parse_obj_as
from pydantic.error_wrappers import ErrorWrapper


class SortDirection(IntEnum):
    ASC = 1
    DESC = -1


CursorSort = List[Tuple[str, SortDirection]]


@dataclass(frozen=True)
class Filter:
    field: str


@dataclass(frozen=True)
class IdsFilter(Filter):
    values: FrozenSet


class TextFilterOperation(Enum):
    EQ = '='
    NEQ = '!'
    START = '^'
    END = '$'
    CONTAIN = '%'


@dataclass(frozen=True)
class TextFilter(Filter):
    op: TextFilterOperation
    value: str


class OrderFilterOperation(Enum):
    EQ = '='
    NEQ = '!'
    GT = '>'
    GTE = '>='
    LT = '<'
    LTE = '<='


@dataclass(frozen=True)
class OrderFilter(Filter):
    op: OrderFilterOperation
    value: Any


@dataclass(frozen=True)
class IntFilter(OrderFilter):
    value: int


@dataclass(frozen=True)
class DecimalFilter(OrderFilter):
    value: Decimal


@dataclass(frozen=True)
class DatetimeFilter(OrderFilter):
    value: datetime


class FilterField:
    name: str

    @classmethod
    def parse_filter_value(cls, field: str, value: Json) -> Filter:
        if isinstance(value, str):
            return TextFilter(field=field, op=TextFilterOperation.EQ, value=value)
        else:
            parsed = parse_obj_as(Tuple[str, str], value)
            op = TextFilterOperation(parsed[0])
            return TextFilter(field=field, op=op, value=parsed[1])


class OrderedFilterField(FilterField):
    filter_type: Type[OrderFilter]

    # noinspection PyArgumentList
    @classmethod
    def parse_filter_value(cls, field: str, value: Json) -> Filter:
        value_type = cls.filter_type.__annotations__['value']
        try:
            parsed = parse_obj_as(value_type, value)
            return cls.filter_type(field=field, op=OrderFilterOperation.EQ, value=parsed)
        except ValidationError:
            parsed = parse_obj_as(Tuple[str, value_type], value)
            op = OrderFilterOperation(parsed[0])
            return cls.filter_type(field=field, op=op, value=parsed[1])


class IntFilterField(OrderedFilterField):
    filter_type = IntFilter


class DecimalFilterField(OrderedFilterField):
    filter_type = DecimalFilter


class DatetimeFilterField(OrderedFilterField):
    filter_type = DatetimeFilter


class SearchQueryMeta(abc.ABCMeta):

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        filters = {}
        for base in bases:
            base_filters = getattr(base, '__filters__', None)
            if base_filters:
                filters.update(base_filters)
        for key, field in cls.__dict__.items():
            if isinstance(field, FilterField):
                filters[key] = field
        cls.__filters__ = filters
        return cls


class SearchQuery(metaclass=SearchQueryMeta):
    __filters__: Dict[str, FilterField]
    id_type = str

    def __init__(
            self,
            filter: Optional[Json] = Query(None),
            offset: Optional[id_type] = Query(None),
            limit: conint(ge=1, lt=251) = Query(20),
            with_count: bool = False
    ):
        self.filter: FrozenSet[Filter] = frozenset(self.parse_filter_values(filter)) if filter else frozenset()
        self.limit = limit
        try:
            self.offset = parse_obj_as(self.id_type, offset) if offset else None
        except ValidationError as e:
            raise RequestValidationError([ErrorWrapper(e, 'filter')])
        self.with_count = with_count

    def parse_filter_values(self, values: Json) -> Iterable[Filter]:
        filter_values = parse_obj_as(Dict[str, Any], values)
        if 'ids' in filter_values:
            return self.parse_ids(filter_values['ids']),
        result = []
        for key, value in filter_values.items():
            filter = self.__filters__.get(key)
            if filter:
                result.append(filter.parse_filter_value(key, value))
            else:
                raise RequestValidationError([
                    ErrorWrapper(ValueError(f'Bad filter value {{{key}: {value}}}'), 'filter')
                ])
        return result

    def parse_ids(self, values: Json) -> Filter:
        parsed = parse_obj_as(FrozenSet[self.id_type], values)
        return IdsFilter(field='ids', values=parsed)


ItemsListType = TypeVar('ItemsListType')


class BaseItemsList(BaseModel):
    items: List[BaseModel]
    offset: Optional[str]
    last: Optional[str]
    limit: int
    count: Optional[int] = None
