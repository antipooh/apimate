import asyncio
import logging
from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, Optional, Type, TypeVar

import httpx
# noinspection PyProtectedMember
from httpx._types import HeaderTypes, QueryParamTypes, RequestData, RequestFiles
from pydantic import BaseModel, ValidationError

logger = logging.getLogger(__name__)

DataModel = TypeVar("DataModel", bound=BaseModel)


@dataclass
class RequestResult:
    success: bool
    data: Optional[DataModel] = None
    error: Optional[Exception] = None


class BadResponseCode(Exception):
    pass


class HttpClient:
    base_url: str
    max_try_count = 3

    def __init__(self, base_url: str, concurrency: int = 10):
        self.base_url = base_url
        self.limit = asyncio.Semaphore(value=concurrency)

    def create_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, http2=True)

    async def request_json_api(self,
                               method: str,
                               path: str,
                               return_type: Type[DataModel],
                               *,
                               data: RequestData = None,
                               files: RequestFiles = None,
                               json: Any = None,
                               params: QueryParamTypes = None,
                               headers: HeaderTypes = None,
                               success_code: int = 200,
                               max_try_count: Optional[int] = None
                               ) -> RequestResult:
        if not max_try_count:
            max_try_count = self.max_try_count
        success = False
        try_count = 1
        while not success:
            async with self.limit:
                async with self.create_client() as client:
                    try:
                        response = await client.request(method, path,
                                                        data=data,
                                                        json=json,
                                                        files=files,
                                                        params=params,
                                                        headers=headers)
                        if response.status_code != success_code:
                            raise BadResponseCode(f'{response.status_code}: {response.text[:255]}')
                        data = return_type.parse_obj(response.json())
                        return RequestResult(success=True, data=data)
                    except (httpx.HTTPError, JSONDecodeError, ValidationError, BadResponseCode) as e:
                        if try_count >= max_try_count:
                            logger.error('Get error for %s %s: %s', self.base_url, path, e, exc_info=True)
                            return RequestResult(success=False, error=e)
                        else:
                            logger.info('Get error for %s %s: %s', self.base_url, path, e, exc_info=True)
                            try_count += 1
