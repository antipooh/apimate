import asyncio
import re
from collections import defaultdict
from functools import cached_property, reduce
from typing import AsyncGenerator, Awaitable, Callable, Dict, List, Optional, Set, Tuple, Type, cast

from bson import ObjectId
from pymongo.collection import Collection

from apimate.query import (BaseItemsList, BoolFilter, DatetimeFilter, DecimalFilter, Filter, IdsFilter, IntFilter,
                           ItemsListType, OrderFilter, OrderFilterOperation, RefFilter,
                           SearchQuery, TextFilter, TextFilterOperation)
from .crud import SavedModel, TypeSelector
from .types import from_mongo


def filter_ref(filter: RefFilter) -> Tuple[str, dict]:
    return filter.field, {'$eq': filter.value}


def filter_bool(filter: BoolFilter) -> Tuple[str, dict]:
    return filter.field, {'$eq': filter.value}


def filter_ids(filter: IdsFilter) -> Tuple[str, dict]:
    return '_id', {'$in': [ObjectId(x) for x in filter.values]}


def filter_text(filter: TextFilter) -> Tuple[str, dict]:
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


def filter_ordered(filter: OrderFilter) -> Tuple[str, dict]:
    return filter.field, {{
                              OrderFilterOperation.EQ: '$eq',
                              OrderFilterOperation.NEQ: '$ne',
                              OrderFilterOperation.LT: '$lt',
                              OrderFilterOperation.LTE: '$lte',
                              OrderFilterOperation.GT: '$gt',
                              OrderFilterOperation.GTE: '$gte',
                          }[filter.op]: filter.value}


class MongodbSearchBuilder:
    filter_map: Dict[Type[Filter], Callable[[Filter], Tuple[str, dict]]] = {
        RefFilter: filter_ref,
        IdsFilter: filter_ids,
        BoolFilter: filter_bool,
        TextFilter: filter_text,
        IntFilter: filter_ordered,
        DecimalFilter: filter_ordered,
        DatetimeFilter: filter_ordered,
    }

    def __init__(self, query: SearchQuery):
        self.query: SearchQuery = query

    @cached_property
    def filter(self) -> dict:
        find_request = defaultdict(dict)
        for filter in self.query.filter:
            filter_builder = self.filter_map[filter.__class__]
            if filter_builder:
                field, cond = filter_builder(filter)
                find_request[field].update(cond)
            else:
                raise NotImplementedError(f'Transform for filter {filter.__class__} not implemented')
        return dict(find_request)

    @property
    def sort(self) -> Tuple[str, int]:
        if self.query.sort:
            return self.query.sort[0], self.query.sort[1].value

    @property
    def skip(self) -> Optional[int]:
        if self.query.page > 1:
            return (self.query.page - 1) * self.query.limit

    @property
    def limit(self) -> int:
        return self.query.limit


async def list_model(collection: Collection,
                     type_selector: TypeSelector,
                     query: SearchQuery,
                     result_type: ItemsListType) -> BaseItemsList:
    query_builder = MongodbSearchBuilder(query)
    count = None
    if query.with_count:
        count = await collection.count_documents(query_builder.filter)
    cursor = collection.find(query_builder.filter)
    try:
        if query_builder.sort:
            cursor = cursor.sort(*query_builder.sort)
        if query_builder.skip:
            cursor = cursor.skip(query_builder.skip)
        cursor = cursor.limit(query_builder.limit)
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
