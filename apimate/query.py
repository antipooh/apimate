import abc
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from fastapi import Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Json, conint, parse_obj_as
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

    def parse_value(self, value: Union[Tuple[str, Any], Any]) -> Filter:
        if isinstance(value, Tuple):
            op = TextFilterOperation(value[0])
            value = str(value[1])
        else:
            op = TextFilterOperation.EQ
            value = str(value)
        return TextFilter(field=self.name, op=op, value=value)


class OrderedFilterField(FilterField):
    filter_type: Type[OrderFilter]

    # noinspection PyArgumentList
    def parse_value(self, value: Union[Tuple[str, Any], Any]) -> Filter:
        value_type = self.filter_type.__annotations__['value']
        if isinstance(value, Tuple):
            op = OrderFilterOperation(value[0])
            value = parse_obj_as(value_type, value[1])
        else:
            op = OrderFilterOperation.EQ
            value = parse_obj_as(value_type, value)
        return self.filter_type(field=self.name, op=op, value=value)


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
                field.name = key
        cls.__filters__ = filters
        return cls


FilterJson = Dict[str, Union[str, List[Any], Dict[str, Any]]]


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
        try:
            self.filter: FrozenSet[Filter] = frozenset(self.parse_filter_values(filter)) if filter else frozenset()
            self.limit = limit
            self.offset = parse_obj_as(self.id_type, offset) if offset else None
            self.with_count = with_count
        except Exception as e:
            self.raise_request_error(e)

    def raise_request_error(self, e: Exception) -> None:
        raise RequestValidationError([ErrorWrapper(e, 'filter')])

    def parse_filter_values(self, values: Json) -> Iterable[Filter]:
        filter = parse_obj_as(FilterJson, values)
        result = []
        for field_name, condition in filter.items():
            if field_name == 'ids':
                return [self.parse_ids(condition)]
            else:
                field = self.__filters__.get(field_name)
                if field:
                    if isinstance(condition, dict):
                        result.extend(field.parse_value(x) for x in condition.items())
                    else:
                        result.append(field.parse_value(condition))
                else:
                    raise KeyError(f'Bad field in filter "{field_name}"')
        return result

    def parse_ids(self, values: List[Any]) -> Filter:
        parsed = parse_obj_as(FrozenSet[self.id_type], values)
        return IdsFilter(field='ids', values=parsed)


ItemsListType = TypeVar('ItemsListType')


class BaseItemsList(BaseModel):
    items: List[BaseModel]
    offset: Optional[str]
    last: Optional[str]
    limit: int
    count: Optional[int] = None
