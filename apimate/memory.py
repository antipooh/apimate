import linecache
import logging
import tracemalloc

from starlette.middleware.base import BaseHTTPMiddleware, DispatchFunction
from starlette.requests import Request
from starlette.types import ASGIApp

logger = logging.getLogger('memory')


def log_top(snapshot, key_type='lineno', limit=10):
    snapshot = snapshot.filter_traces((
        tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
        tracemalloc.Filter(False, "<unknown>"),
    ))
    top_stats = snapshot.statistics(key_type)
    logger.info("Top %s lines", limit)
    for index, stat in enumerate(top_stats[:limit], 1):
        frame = stat.traceback[0]
        logger.info("#%s: %s:%s: %.1f KiB", index, frame.filename, frame.lineno, stat.size / 1024)
        line = linecache.getline(frame.filename, frame.lineno).strip()
        if line:
            logger.info('    %s', line)

    other = top_stats[limit:]
    if other:
        size = sum(stat.size for stat in other)
        logger.info("%s other: %.1f KiB", len(other), size / 1024)
    total = sum(stat.size for stat in top_stats)
    logger.info("Total allocated size: %.1f KiB", total / 1024)


class MemoryLogMiddleware(BaseHTTPMiddleware):

    def __init__(self, app: ASGIApp, dispatch: DispatchFunction = None, log_top=False) -> None:
        super().__init__(app, dispatch)
        self.log_top_memory = log_top

    async def dispatch(self, request: Request, call_next):
        logger.info('REQUEST %s', request.url)
        tracemalloc.start()
        response = await call_next(request)
        if self.log_top_memory:
            snapshot = tracemalloc.take_snapshot()
            log_top(snapshot)
        else:
            current, peak = tracemalloc.get_traced_memory()
            logger.info("Current memory usage is %sMB; Peak was %sMB",
                        current / 10 ** 6, peak / 10 ** 6)
        tracemalloc.stop()
        return response
