import abc
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, IntEnum, IntFlag
from typing import Any, Dict, FrozenSet, Iterable, List, Literal, Optional, Tuple, Type, TypeVar, Union

from fastapi import Query
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel, Json, conint, parse_obj_as
from pydantic.error_wrappers import ErrorWrapper


class SortDirection(IntEnum):
    ASC = 1
    DESC = -1


Sort = Tuple[str, SortDirection]

CursorSort = List[Sort]


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


class FieldSort(IntFlag):
    NO = 0
    ASC = 1
    DESC = 2


class QueryField:
    name: str

    def __init__(self, sort: Optional[FieldSort] = None) -> None:
        self.sort = sort or FieldSort.NO

    def parse_value(self, value: Union[Tuple[str, Any], Any]) -> Filter:
        if isinstance(value, Tuple):
            op = TextFilterOperation(value[0])
            value = str(value[1])
        else:
            op = TextFilterOperation.EQ
            value = str(value)
        return TextFilter(field=self.name, op=op, value=value)

    def can_sort(self, direction: SortDirection) -> bool:
        return direction.ASC and FieldSort.ASC in self.sort or direction.DESC and FieldSort.DESC in self.sort


class OrderedQueryField(QueryField):
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


class IntQueryField(OrderedQueryField):
    filter_type = IntFilter


class DecimalQueryField(OrderedQueryField):
    filter_type = DecimalFilter


class DatetimeQueryField(OrderedQueryField):
    filter_type = DatetimeFilter


class SearchQueryMeta(abc.ABCMeta):

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)
        fields = {}
        default_sort: Optional[Sort] = None
        for base in bases:
            base_fields = getattr(base, '__fields__', None)
            if base_fields:
                fields.update(base_fields)
            default_sort = getattr(base, 'default_sort', default_sort)
        for key, field in cls.__dict__.items():
            if isinstance(field, QueryField):
                fields[key] = field
                field.name = key
        cls.__fields__ = fields
        default_sort = getattr(cls, 'default_sort', default_sort)
        if default_sort:
            field = fields.get(default_sort[0])
            if not field or not field.can_sort(default_sort[1]):
                raise ValueError(f'Can`t use default sort by {default_sort}')
        return cls


FilterJson = Dict[str, Union[str, List[Any], Dict[str, Any]]]
SortTuple = Tuple[str, Literal['asc', 'dsc']]


class SearchQuery(metaclass=SearchQueryMeta):
    __fields__: Dict[str, QueryField]
    id_type = str
    default_sort: Optional[Sort] = None

    def __init__(
            self,
            filter: Optional[Json] = Query(None),
            sort: Optional[Union[Json, str]] = Query(None),
            page: int = Query(0),
            limit: conint(ge=1, lt=251) = Query(20),
            with_count: bool = Query(False, alias='withCount')
    ):
        try:
            self.filter: FrozenSet[Filter] = frozenset(self.parse_filter_values(filter)) if filter else frozenset()
            self.sort: Optional[Sort] = self.parse_sort_value(sort) if sort else self.default_sort
            self.page = page
            self.limit = limit
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
                field = self.__fields__.get(field_name)
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

    def parse_sort_value(self, value: Union[Json, str]) -> Optional[Sort]:
        if isinstance(value, str):
            sort = value, SortDirection.ASC
        else:
            parsed = parse_obj_as(SortTuple, value)
            field_name, verbose_direction = parsed
            sort = field_name, {'asc': SortDirection.ASC, 'dsc': SortDirection.DESC}[verbose_direction]
        field = self.__fields__.get(sort[0])
        if field:
            if field.can_sort(sort[1]):
                return sort
            else:
                verbose_direction = {SortDirection.ASC: 'ascent', SortDirection.DESC: 'descent'}[sort[1]]
                raise ValueError(f'Can`t sort by "{sort[0]}" in {verbose_direction}')
        else:
            raise KeyError(f'Bad field in sort "{sort[0]}"')


ItemsListType = TypeVar('ItemsListType')


class BaseItemsList(BaseModel):
    items: List[BaseModel]
    page: Optional[int]
    limit: int
    count: Optional[int] = None
