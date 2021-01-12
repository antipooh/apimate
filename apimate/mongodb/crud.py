from typing import AsyncGenerator, Optional, Type, TypeVar

from bson import ObjectId
from pydantic import BaseModel
from pymongo.collection import Collection

from .types import InDBModel, from_mongo, to_mongo
from ..query import CursorSort

PydanticModel = TypeVar("PydanticModel", bound=BaseModel)

SavedModel = TypeVar("SavedModel", bound=InDBModel)


async def get_model(collection: Collection, model_type: Type[SavedModel], identity: str) -> Optional[SavedModel]:
    doc = await collection.find_one({'_id': ObjectId(identity)})
    if doc:
        return model_type.parse_obj(from_mongo(doc))


async def insert_model(collection: Collection, model_type: Type[SavedModel], insert: PydanticModel) -> SavedModel:
    result = await collection.insert_one(to_mongo(insert.dict()))
    doc = await collection.find_one({'_id': ObjectId(result.inserted_id)})
    return model_type.parse_obj(from_mongo(doc))


async def update_model_one(collection: Collection, identity: str, update: PydanticModel) -> bool:
    update_data = {'$set': to_mongo(update.dict(skip_defaults=True))}
    result = await collection.update_one({'_id': ObjectId(identity)}, update_data)
    return result.modified_count > 0 if result.acknowledged else False


async def update_model(collection: Collection, query: dict, update: dict) -> bool:
    result = await collection.update_many(query, update)
    return result.modified_count > 0 if result.acknowledged else False


async def search_model(collection: Collection,
                       model_type: Type[SavedModel],
                       query: dict, sort: Optional[CursorSort] = None) -> AsyncGenerator[SavedModel, None]:
    cursor = collection.find(query, sort=sort)
    async for doc in cursor:
        yield model_type.parse_obj(from_mongo(doc))


async def search_model_one(collection: Collection,
                           model_type: Type[SavedModel],
                           query: dict) -> Optional[SavedModel]:
    doc = await collection.find_one(query)
    if doc:
        return model_type.parse_obj(from_mongo(doc))


async def remove_model(collection: Collection, identity: str) -> bool:
    result = await collection.delete_one({'_id': ObjectId(identity)})
    return result.deleted_count > 0 if result.acknowledged else False
