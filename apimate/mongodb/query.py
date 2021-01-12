from typing import Type

from pymongo.collection import Collection

from apimate.query import BaseItemsList, ItemsListType, SearchQuery
from .crud import SavedModel
from .types import from_mongo


async def list_model(collection: Collection,
                     model_type: Type[SavedModel], query: SearchQuery,
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
        items = [model_type.parse_obj(from_mongo(doc)) async for doc in cursor]
    finally:
        await cursor.close()
    return result_type(items=items, offset=query.offset, limit=query.limit, count=count)
