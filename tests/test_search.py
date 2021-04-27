from decimal import Decimal

import pytest

from apimate.query import (DecimalFilter, DecimalFilterField, FilterField, IdsFilter, IntFilter, IntFilterField,
                           OrderFilterOperation, SearchQuery, TextFilter, TextFilterOperation)


class FakeSearchQuery(SearchQuery):
    text = FilterField()


def test_collect_filters():
    query = FakeSearchQuery(filter=None)
    assert len(query.__filters__) == 1
    filter = query.__filters__['text']
    assert isinstance(filter, FilterField)


class InheritedFakeSearchQuery(FakeSearchQuery):
    value = FilterField()


def test_collect_base_filters():
    query = InheritedFakeSearchQuery(filter=None)
    assert set(query.__filters__) == {'text', 'value'}
    assert isinstance(query.__filters__['text'], FilterField)
    assert isinstance(query.__filters__['value'], FilterField)


def test_parse_ids():
    search = SearchQuery(filter={"ids": ['A', 'B', 'C']})
    assert search.filter == {IdsFilter(field='ids', values=frozenset(('A', 'B', 'C')))}


@pytest.mark.parametrize('filter, operation, value', (
        ('Ok', TextFilterOperation.EQ, 'Ok'),
        (['=', 'Yes'], TextFilterOperation.EQ, 'Yes'),
        (['!', 'No'], TextFilterOperation.NEQ, 'No'),
        (['%', 'No'], TextFilterOperation.CONTAIN, 'No'),
))
def test_text_filter(filter, operation, value):
    search = FakeSearchQuery(filter={'text': filter})
    assert search.filter == {TextFilter(field='text', op=operation, value=value)}


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, 2),
        (['>=', 123], OrderFilterOperation.GTE, 123),
))
def test_int_filter(filter, operation, value):
    result = IntFilterField.parse_filter_value('field', filter)
    assert result == IntFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, Decimal('2')),
        (['>=', '123.023'], OrderFilterOperation.GTE, Decimal('123.023')),
))
def test_decimal_filter(filter, operation, value):
    result = DecimalFilterField.parse_filter_value('field', filter)
    assert result == DecimalFilter(field='field', op=operation, value=value)


@pytest.mark.parametrize('filter, operation, value', (
        (2, OrderFilterOperation.EQ, Decimal('2')),
        (['>=', '123.023'], OrderFilterOperation.GTE, Decimal('123.023')),
))
def test_datetime_filter(filter, operation, value):
    result = DecimalFilterField.parse_filter_value('field', filter)
    assert result == DecimalFilter(field='field', op=operation, value=value)
