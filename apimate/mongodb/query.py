import re
from collections import defaultdict
from functools import cached_property
from typing import Tuple, Type

from bson import ObjectId
from pydantic import constr
from pymongo.collection import Collection

from apimate.query import (BaseItemsList, IdsFilter, ItemsListType, OrderFilter, OrderFilterOperation, SearchQuery,
                           TextFilter, TextFilterOperation)
from apimate.types import ID_STRING
from .crud import TypeSelector
from .types import from_mongo


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
            elif isinstance(filter, OrderFilter):
                field, cond = self.filter_ordered(filter)
            if field and cond:
                find_request[field].update(cond)
            else:
                raise NotImplementedError(f'Transform for filter {filter.__class__} not implemented')
        return dict(find_request)

    @cached_property
    def paged_find(self) -> dict:
        result = self.find
        if self.offset:
            last_id = ObjectId(self.offset)
            if '_id' in result:
                result['_id']['$gt'] = last_id
            else:
                result['_id'] = {'$gt': last_id}
        return result

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


async def list_model(collection: Collection,
                     type_selector: TypeSelector,
                     query: MongodbSearchQuery,
                     result_type: Type[BaseItemsList]) -> ItemsListType:
    count = None
    if query.with_count:
        count = await collection.count_documents(query.find)
    cursor = collection.find(query.paged_find)
    try:
        if query.sort:
            cursor = cursor.sort(query.sort)
        cursor = cursor.limit(query.limit)
        items = []
        async for doc in cursor:
            data = from_mongo(doc)
            model_type = type_selector(data)
            items.append(model_type.parse_obj(data))
    finally:
        await cursor.close()
    return result_type(items=items,
                       offset=str(query.offset) if query.offset else None,
                       last=str(items[-1].id) if items else None,
                       limit=query.limit,
                       count=count)
