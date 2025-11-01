from types import FunctionType
from typing import Callable
import threading
from contextlib import contextmanager

from .redis import RedisSessionManager
import redis

# Thread-local storage
_thread_local = threading.local()


class Request:
    """Request-based empty object"""
    pass

class WebContext:
    def __init__(self, manager: 'ContextManager', token):
        self.token = token
        self.manager = manager
        self.db_session = None
        self.manager.redis

    def __enter__(self):
        if self.token:
            self.session = self.manager.web_session_man.connect(self.token)
        else:
            self.token, self.session = self.manager.web_session_man.new()


        self.request = Request()

        self.db_session = self.manager.session_maker()
        self.db_session.begin()
        # Set thread-local values
        _thread_local.web_context = self
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.manager.web_session_man.disconnect(self.session, self.token)
        if not any((exc_val, exc_tb, exc_type)):
            self.db_session.commit()
        else:
            self.db_session.rollback()

        # Clean up thread-local values
        _thread_local.web_context = None

class ContextManager:
    """JSAlchemy request context manager using thread.local instead of ContextVar."""

    def __init__(self, session_maker: Callable,
                 redis_connection: redis.Redis | str = 'redis://localhost:6379/0',
                 auto_commit: bool = False, trace_changes: bool = False, change_call_back: FunctionType = None):
        from .cache import setup_cache
        setup_cache(redis_connection=redis_connection)
        self.session_maker = session_maker
        self.redis = redis.Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection
        self.web_session_man = RedisSessionManager(redis_connection)
        self.auto_commit = auto_commit
        self.trace_changes = trace_changes
        self.change_call_back = change_call_back
        if change_call_back or trace_changes:
            from ..interceptors import ChangeInterceptor
            self.change_interceptor = ChangeInterceptor(change_call_back, request=request)
        else:
            self.change_interceptor = None

    def __enter__(self):
        _thread_local.context = WebContext()

    def __call__(self, token: str | None = None):
        return WebContext(self, token)


class ContextProxy:
    def __init__(self, name: str):
        self.name = name


    def update(self, obj: object) -> None:
        setattr(_thread_local.web_context, self.name, obj)

    def __getattr__(self, item: str):
        obj = getattr(_thread_local.web_context, self.name)
        return getattr(obj, item, None)

    def __setattr__(self, key, value):
        if key in ('name', 'update'):
            super().__setattr__(key, value)
            return
        obj = getattr(_thread_local.web_context, self.name)
        setattr(obj, key, value)

    __getitem__ = __getattr__
    __setitem__ = __setattr__

# Create global proxies using thread-local storage
session = ContextProxy('session')
request = ContextProxy('request')
db = ContextProxy('db_session')
redis = ContextProxy('redis')
