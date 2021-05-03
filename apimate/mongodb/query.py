import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property, reduce
from typing import Any, AsyncGenerator, Awaitable, Callable, Dict, List, Set, Tuple, Union, cast

from bson import ObjectId
from pydantic import constr, parse_obj_as
from pymongo.collection import Collection

from apimate.query import (BaseItemsList, BoolFilter, Filter, IdsFilter, ItemsListType, OrderFilter,
                           OrderFilterOperation, QueryField, SearchQuery, TextFilter, TextFilterOperation)
from .crud import SavedModel, TypeSelector
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
                     result_type: ItemsListType) -> BaseItemsList:
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
    # noinspection PyCallingNonCallable
    return result_type(items=items,
                       page=query.page,
                       limit=query.limit,
                       count=count)


SearchAwaitable = Callable[[dict], Awaitable[AsyncGenerator[SavedModel, None]]]


class Relation:

    def __init__(self, id_prop: str, target_prop: str, search: SearchAwaitable) -> None:
        self.id_prop = id_prop
        self.target_prop = target_prop
        self.search = search
        self.ids: Set[str] = set()
        self.models: Dict[str, SavedModel] = {}

    def extract_id(self, item: SavedModel) -> None:
        rel_id = getattr(item, self.id_prop, None)
        if rel_id:
            self.ids.add(rel_id)

    async def load(self) -> None:
        if self.ids:
            async for model in await self.search({'_id': {'$in': [ObjectId(id) for id in self.ids]}}):
                self.models[model.id] = model

    def inject(self, item: SavedModel) -> SavedModel:
        rel_id = getattr(item, self.id_prop, None)
        if rel_id:
            setattr(item, self.target_prop, self.models.get(rel_id))
        else:
            setattr(item, self.target_prop, None)
        return item


async def inject_relations(model: SavedModel, relations: List[Relation]) -> SavedModel:
    for relation in relations:
        relation.extract_id(model)
    await asyncio.gather(*(relation.load() for relation in relations), return_exceptions=True)
    return reduce(lambda x, r: r.inject(x), relations, model)


async def inject_list_relations(items: BaseItemsList, relations: List[Relation]) -> BaseItemsList:
    for item in items.items:
        for relation in relations:
            relation.extract_id(cast(SavedModel, item))
    await asyncio.gather(*(relation.load() for relation in relations), return_exceptions=True)
    items.items = [reduce(lambda x, r: r.inject(x), relations, item) for item in items.items]
    return items
