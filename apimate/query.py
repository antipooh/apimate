import re
from enum import Enum, IntEnum
from typing import List, Optional, Tuple, TypeVar

from pydantic import BaseModel, conint


class SearchQuery(BaseModel):
    offset: conint(ge=0) = 0
    limit: conint(ge=1, lt=251) = 20
    with_count: bool = False

    class Config:
        anystr_strip_whitespace = True

    def query(self):
        result = {}
        for name in self.__fields_set__:
            if name not in {'offset', 'limit', 'with_count'}:
                field = self.__fields__[name]
                extra = field.field_info.extra
                value = getattr(self, name)
                if value is None:
                    continue
                if isinstance(value, Enum):
                    value = value.value
                regex = extra.get('search_regex')
                if regex:
                    value = {'$regex': re.compile(regex.format(value)) if isinstance(regex, str) else value}
                result[name] = value
        return result


ItemsListType = TypeVar('ItemsListType')


class BaseItemsList(BaseModel):
    items: List[BaseModel]
    offset: int
    limit: int
    sort: Optional[str] = None
    count: Optional[int] = None


class SortDirection(IntEnum):
    ASC = 1
    DESC = -1


CursorSort = List[Tuple[str, SortDirection]]
