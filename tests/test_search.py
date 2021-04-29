from datetime import datetime
from decimal import Decimal

import pytest

from apimate.query import (DatetimeFilter, DatetimeFilterField, DecimalFilter, DecimalFilterField, FilterField,
                           IdsFilter, IntFilter,
                           IntFilterField,
                           OrderFilterOperation, SearchQuery, TextFilter, TextFilterOperation)


class FakeSearchQuery(SearchQuery):
    text = FilterField()


def test_collect_filters():
    query = FakeSearchQuery(filter=None, offset=None)
    assert len(query.__filters__) == 1
    filter = query.__filters__['text']
    assert isinstance(filter, FilterField)


class InheritedFakeSearchQuery(FakeSearchQuery):
    value = FilterField()


def test_collect_base_filters():
    query = InheritedFakeSearchQuery(filter=None, offset=None)
    assert set(query.__filters__) == {'text', 'value'}
    assert isinstance(query.__filters__['text'], FilterField)
    assert isinstance(query.__filters__['value'], FilterField)


def test_parse_ids():
    search = SearchQuery(filter={"ids": ['A', 'B', 'C']}, offset=None)
    assert search.filter == {IdsFilter(field='ids', values=frozenset(('A', 'B', 'C')))}


@pytest.mark.parametrize('filter, operation, value', (
        ('Ok', TextFilterOperation.EQ, 'Ok'),
        ({'=': 'Yes'}, TextFilterOperation.EQ, 'Yes'),
        ({'!': 'No'}, TextFilterOperation.NEQ, 'No'),
        ({'%': 'No'}, TextFilterOperation.CONTAIN, 'No'),
))
def test_text_filter(filter, operation, value):
    search = FakeSearchQuery(filter={'text': filter}, offset=None)
    assert search.filter == frozenset([TextFilter(field='text', op=operation, value=value)])


def test_multi_filter():
    search = FakeSearchQuery(filter={'text': {'=': 'A', '!': 'B'}},
                             offset=None)
    assert search.filter == {TextFilter(field='text', op=TextFilterOperation.EQ, value='A'),
                             TextFilter(field='text', op=TextFilterOperation.NEQ, value='B')}


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, 2),
        (('>=', '123'), OrderFilterOperation.GTE, 123),
))
def test_int_filter(filter, operation, value):
    field = IntFilterField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == IntFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, Decimal('2')),
        (('>=', '123.023'), OrderFilterOperation.GTE, Decimal('123.023')),
))
def test_decimal_filter(filter, operation, value):
    field = DecimalFilterField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == DecimalFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        ('2021-04-29T14:05', OrderFilterOperation.EQ, datetime(2021, 4, 29, 14, 5)),
        (('>=', '2021-04-29T00:00'), OrderFilterOperation.GTE, datetime(2021, 4, 29)),
))
def test_datetime_filter(filter, operation, value):
    field = DatetimeFilterField()
    field.name = 'field'
    result = field.parse_value(filter)
    assert result == DatetimeFilter(field='field', op=operation, value=value)
