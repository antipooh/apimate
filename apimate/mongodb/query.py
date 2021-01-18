from typing import Type

from pymongo.collection import Collection

from apimate.query import BaseItemsList, ItemsListType, SearchQuery
from .crud import SavedModel, TypeSelector
from .types import from_mongo


async def list_model(collection: Collection,
                     type_selector: TypeSelector,
                     query: SearchQuery,
                     result_type: Type[BaseItemsList]) -> ItemsListType:
    count = None
    query_dict = query.query()
    if query.with_count:
        count = await collection.count_documents(query_dict)
    cursor = collection.find(query_dict)
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
