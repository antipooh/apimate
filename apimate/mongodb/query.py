import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Tuple, Type, Union

from bson import ObjectId
from pydantic import constr, parse_obj_as
from pymongo.collection import Collection

from apimate.query import (BaseItemsList, BoolFilter, Filter, IdsFilter, ItemsListType, OrderFilter,
                           OrderFilterOperation, QueryField, SearchQuery, TextFilter, TextFilterOperation)
from .crud import TypeSelector
from .types import from_mongo
from ..types import ID_STRING


@dataclass(frozen=True)
class RefFilter(Filter):
    value: ObjectId


class RefQueryField(QueryField):

    def parse_value(self, value: Union[Tuple[str, Any], Any]) -> Filter:
        return RefFilter(field=self.name, value=ObjectId(parse_obj_as(constr(regex=ID_STRING), value)))


class MongodbSearchQuery(SearchQuery):
    id_type = constr(regex=ID_STRING)
    order_filter_map = {
        OrderFilterOperation.EQ: '$eq',
        OrderFilterOperation.NEQ: '$ne',
        OrderFilterOperation.LT: '$lt',
        OrderFilterOperation.LTE: '$lte',
        OrderFilterOperation.GT: '$gt',
        OrderFilterOperation.GTE: '$gte',
    }

    @cached_property
    def find(self) -> dict:
        find_request = defaultdict(dict)
        for filter in self.filter:
            field, cond = None, None
            if isinstance(filter, IdsFilter):
                field, cond = self.filter_ids(filter)
                return {field: cond}  # If has ids list, d`not use any other filters
            elif isinstance(filter, TextFilter):
                field, cond = self.filter_text(filter)
            elif isinstance(filter, (BoolFilter, RefFilter)):
                field, cond = self.filter_equal(filter)
            elif isinstance(filter, OrderFilter):
                field, cond = self.filter_ordered(filter)
            if field and cond:
                find_request[field].update(cond)
            else:
                raise NotImplementedError(f'Transform for filter {filter.__class__} not implemented')
        return dict(find_request)

    @property
    def sorting(self) -> Tuple[str, int]:
        return self.sort[0], self.sort[1].value

    def filter_ids(self, filter: IdsFilter) -> Tuple[str, dict]:
        return '_id', {'$in': [ObjectId(x) for x in filter.values]}

    def filter_text(self, filter: TextFilter) -> Tuple[str, dict]:
        if filter.op == TextFilterOperation.EQ:
            cond = {'$eq': filter.value}
        elif filter.op == TextFilterOperation.NEQ:
            cond = {'$ne': filter.value}
        else:
            value = filter.value
            if filter.op == TextFilterOperation.START:
                value = f'^{value}'
            elif filter.op == TextFilterOperation.END:
                value = f'{value}$'
            cond = {'$regex': re.compile(value, re.IGNORECASE | re.UNICODE)}
        return filter.field, cond

    def filter_ordered(self, filter: OrderFilter) -> Tuple[str, dict]:
        return filter.field, {self.order_filter_map[filter.op]: filter.value}

    def filter_equal(self, filter: Union[BoolFilter, RefFilter]) -> Tuple[str, dict]:
        return filter.field, {'$eq': filter.value}


async def list_model(collection: Collection,
                     type_selector: TypeSelector,
                     query: MongodbSearchQuery,
                     result_type: Type[BaseItemsList]) -> ItemsListType:
    count = None
    if query.with_count:
        count = await collection.count_documents(query.find)
    cursor = collection.find(query.find)
    try:
        if query.sort:
            cursor = cursor.sort(*query.sort)
        if query.page > 1:
            cursor = cursor.skip((query.page - 1) * query.limit)
        cursor = cursor.limit(query.limit)
        items = []
        async for doc in cursor:
            data = from_mongo(doc)
            model_type = type_selector(data)
            items.append(model_type.parse_obj(data))
    finally:
        await cursor.close()
    return result_type(items=items,
                       page=query.page,
                       limit=query.limit,
                       count=count)
