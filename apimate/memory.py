import linecache
import logging
import os
import tracemalloc

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger('memory')


class MemoryLogMiddleware(BaseHTTPMiddleware):

    async def dispatch(self, request: Request, call_next):
        logger.info('REQUEST %s', request.url)
        tracemalloc.start()
        response = await call_next(request)
        snapshot = tracemalloc.take_snapshot()
        self.log_top(snapshot)
        return response

    @staticmethod
    def log_top(snapshot, key_type='lineno', limit=3):
        snapshot = snapshot.filter_traces((
            tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
            tracemalloc.Filter(False, "<unknown>"),
        ))
        top_stats = snapshot.statistics(key_type)
        other = top_stats[limit:]
        if other:
            size = sum(stat.size for stat in other)
            logger.info("%s other: %.1f KiB", len(other), size / 1024)
        total = sum(stat.size for stat in top_stats)
        logger.info("Total allocated size: %.1f KiB", total / 1024)
