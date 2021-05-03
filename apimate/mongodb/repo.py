from typing import AsyncGenerator, Optional, Type

from motor.motor_asyncio import AsyncIOMotorDatabase
from pymongo.collection import Collection

from apimate.query import BaseItemsList, CursorSort, ItemsListType, SearchQuery
from .crud import (PydanticModel, SavedModel, get_model, insert_model, remove_model, search_model, search_model_one,
                   update_model,
                   update_model_one)
from .query import list_model


class MongoRepo:
    collection_name: str
    model_type: Type[SavedModel]

    def __init__(self, db: AsyncIOMotorDatabase):
        self.db = db

    async def get_collection(self) -> Collection:
        collection = self.db[self.collection_name]
        if not collection:
            collection = await self.db.create_collection(self.collection_name)
        return collection

    def model_type_select(self, doc: dict) -> Type[SavedModel]:
        return self.model_type

    async def _get_model(self, identity: str) -> Optional[SavedModel]:
        return await get_model(await self.get_collection(), self.model_type_select, identity)

    async def _insert_model(self, insert: PydanticModel) -> SavedModel:
        return await insert_model(await self.get_collection(), self.model_type_select, insert)

    async def _update_model_one(self, identity: str, update: PydanticModel) -> bool:
        return await update_model_one(await self.get_collection(), identity, update)

    async def _update_model(self, query: dict, update: dict) -> bool:
        return await update_model(await self.get_collection(), query, update)

    async def _search_model(self, query: dict, sort: Optional[CursorSort] = None) -> AsyncGenerator[SavedModel, None]:
        return search_model(await self.get_collection(), self.model_type_select, query, sort)

    async def _search_model_one(self, query: dict) -> Optional[SavedModel]:
        return await search_model_one(await self.get_collection(), self.model_type_select, query)

    async def _remove_model(self, identity: str) -> bool:
        return await remove_model(await self.get_collection(), identity)

    async def _list_model(self, query: SearchQuery, result_type: Type[BaseItemsList]) -> ItemsListType:
        return await list_model(await self.get_collection(), self.model_type_select, query, result_type)



