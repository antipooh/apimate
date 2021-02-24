from json import JSONDecodeError

import httpx
import pytest
import respx
from httpx import Response
from pydantic import BaseModel, Extra, ValidationError

from apimate.http import BadResponseCode, HttpClient, RequestResult


@pytest.fixture
def http_client() -> HttpClient:
    return HttpClient('https://api.org/', concurrency=1)


class Result(BaseModel):
    message_id: int

    class Config:
        extra = Extra.ignore


class APIResponse(BaseModel):
    result: Result

    class Config:
        extra = Extra.ignore


@pytest.mark.asyncio
async def test_request_ok(http_client):
    with respx.mock:
        respx.post("https://api.org/sendMessage").mock(
            return_value=Response(200, json={'result': {'message_id': 148148}}))
        result = await http_client.request_json_api('post', 'sendMessage', APIResponse)
        assert isinstance(result, RequestResult)
        assert result.success is True
        assert result.data == APIResponse(result=Result(message_id=148148))


@pytest.mark.asyncio
async def test_request_bad_status_code(http_client):
    with respx.mock:
        respx.post("https://api.org/sendMessage").mock(
            return_value=Response(500))
        result = await http_client.request_json_api('post', 'sendMessage', APIResponse)
        assert isinstance(result, RequestResult)
        assert result.success is False
        assert result.data is None
        assert isinstance(result.error, BadResponseCode)
        assert len(respx.calls) == http_client.max_try_count == 3


@pytest.mark.asyncio
async def test_request_bad_data(http_client):
    with respx.mock:
        respx.post("https://api.org/sendMessage").mock(
            return_value=Response(200, json={'result': {'success': 'maybe'}}))
        result = await http_client.request_json_api('post', 'sendMessage', APIResponse)
        assert isinstance(result, RequestResult)
        assert result.success is False
        assert result.data is None
        assert isinstance(result.error, ValidationError)
        assert len(respx.calls) == http_client.max_try_count == 3


@pytest.mark.asyncio
async def test_request_content_mismatch(http_client):
    with respx.mock:
        respx.post("https://api.org/sendMessage").mock(
            return_value=Response(200, content=b'ABC'))
        result = await http_client.request_json_api('post', 'sendMessage', APIResponse)
        assert isinstance(result, RequestResult)
        assert result.success is False
        assert result.data is None
        assert isinstance(result.error, JSONDecodeError)
        assert len(respx.calls) == http_client.max_try_count == 3


@pytest.mark.asyncio
async def test_request_content_mismatch(http_client):
    with respx.mock:
        respx.post("https://api.org/sendMessage").mock(side_effect=httpx.ConnectError)
        result = await http_client.request_json_api('post', 'sendMessage', APIResponse, max_try_count=2)
        assert isinstance(result, RequestResult)
        assert result.success is False
        assert result.data is None
        assert isinstance(result.error, httpx.ConnectError)
        assert len(respx.calls) == 2
