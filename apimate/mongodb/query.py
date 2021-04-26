import re
from enum import Enum
from typing import Type

from pymongo.collection import Collection

from apimate.query import BaseItemsList, ItemsListType, SearchQuery
from .crud import TypeSelector
from .types import from_mongo


def query_2_find(query: SearchQuery) -> dict:
    result = {}
    for name in query.__fields_set__:
        if name not in {'offset', 'limit', 'with_count'}:
            field = query.__fields__[name]
            extra = field.field_info.extra
            value = getattr(query, name)
            if value is None:
                continue
            if isinstance(value, Enum):
                value = value.value
            regex = extra.get('search_regex')
            if regex:
                value = {'$regex': re.compile(regex.format(value)) if isinstance(regex, str) else value}
            result[name] = value
    return result


async def list_model(collection: Collection,
                     type_selector: TypeSelector,
                     query: SearchQuery,
                     result_type: Type[BaseItemsList]) -> ItemsListType:
    count = None
    find_condition = query_2_find(query)
    if query.with_count:
        count = await collection.count_documents(find_condition)
    cursor = collection.find(find_condition)
    try:
        if query.limit:
            cursor = cursor.limit(query.limit)
        if query.offset:
            cursor = cursor.skip(query.offset)
        items = []
        async for doc in cursor:
            data = from_mongo(doc)
            model_type = type_selector(data)
            items.append(model_type.parse_obj(data))
    finally:
        await cursor.close()
    return result_type(items=items, offset=query.offset, limit=query.limit, count=count)
