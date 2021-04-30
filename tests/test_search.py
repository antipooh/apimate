from datetime import datetime
from decimal import Decimal

import pytest

from apimate.query import (DatetimeFilter, DatetimeQueryField, DecimalFilter, DecimalQueryField, FieldSort, IdsFilter,
                           IntFilter, IntQueryField, OrderFilterOperation, QueryField, SearchQuery, SortDirection,
                           TextFilter, TextFilterOperation)


class FakeSearchQuery(SearchQuery):
    text = QueryField(sort=FieldSort.ASC | FieldSort.DESC)
    default_sort = ('text', SortDirection.DESC)


def test_collect_fields():
    query = FakeSearchQuery(filter=None, offset=None, sort=None)
    assert len(query.__fields__) == 1
    filter = query.__fields__['text']
    assert isinstance(filter, QueryField)
    assert query.default_sort == ('text', SortDirection.DESC)


class InheritedFakeSearchQuery(FakeSearchQuery):
    value = QueryField()


def test_collect_base_fields():
    query = InheritedFakeSearchQuery(filter=None, offset=None, sort=None)
    assert set(query.__fields__) == {'text', 'value'}
    assert isinstance(query.__fields__['text'], QueryField)
    assert isinstance(query.__fields__['value'], QueryField)
    assert query.default_sort == ('text', SortDirection.DESC)


def test_bad_default_sort():
    with pytest.raises(ValueError) as e:
        class _(FakeSearchQuery):
            value = QueryField()
            default_sort = ('value', SortDirection.ASC)
    assert str(e.value) == "Can`t use default sort by ('value', <SortDirection.ASC: 1>)"


def test_parse_ids():
    search = SearchQuery(filter={"ids": ['A', 'B', 'C']}, offset=None, sort=None)
    assert search.filter == {IdsFilter(field='ids', values=frozenset(('A', 'B', 'C')))}


@pytest.mark.parametrize('filter, operation, value', (
        ('Ok', TextFilterOperation.EQ, 'Ok'),
        ({'=': 'Yes'}, TextFilterOperation.EQ, 'Yes'),
        ({'!': 'No'}, TextFilterOperation.NEQ, 'No'),
        ({'%': 'No'}, TextFilterOperation.CONTAIN, 'No'),
))
def test_text_filter(filter, operation, value):
    search = FakeSearchQuery(filter={'text': filter}, offset=None, sort=None)
    assert search.filter == frozenset([TextFilter(field='text', op=operation, value=value)])


def test_multi_filter():
    search = FakeSearchQuery(filter={'text': {'=': 'A', '!': 'B'}},
                             offset=None, sort=None)
    assert search.filter == {TextFilter(field='text', op=TextFilterOperation.EQ, value='A'),
                             TextFilter(field='text', op=TextFilterOperation.NEQ, value='B')}


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, 2),
        (('>=', '123'), OrderFilterOperation.GTE, 123),
))
def test_int_filter(filter, operation, value):
    field = IntQueryField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == IntFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, Decimal('2')),
        (('>=', '123.023'), OrderFilterOperation.GTE, Decimal('123.023')),
))
def test_decimal_filter(filter, operation, value):
    field = DecimalQueryField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == DecimalFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        ('2021-04-29T14:05', OrderFilterOperation.EQ, datetime(2021, 4, 29, 14, 5)),
        (('>=', '2021-04-29T00:00'), OrderFilterOperation.GTE, datetime(2021, 4, 29)),
))
def test_datetime_filter(filter, operation, value):
    field = DatetimeQueryField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == DatetimeFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('sort, result', (
        ('text', ('text', SortDirection.ASC)),
        (['text', 'asc'], ('text', SortDirection.ASC)),
        (['text', 'dsc'], ('text', SortDirection.DESC)),
        (None, ('text', SortDirection.DESC)),
))
def test_sort(sort, result):
    search = FakeSearchQuery(filter=None, offset=None, sort=sort)
    assert search.sort == result
