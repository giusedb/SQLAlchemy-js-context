from types import FunctionType
from typing import Callable
from contextvars import ContextVar

from .redis import RedisSessionManager
from redis import Redis

class Request:
    """Request-based empty object"""
    pass

class ContextManager:
    """JSAlchemy request context manager."""

    class Context:
        def __init__(self, manager: 'ContextManager', token):
            self.token = token
            self.manager = manager
            self.db_session = None

        async def __aenter__(self):
            if self.token:
                self.session = await self.manager.web_session_man.connect(self.token)
            else:
                self.token, self.session = await self.manager.web_session_man.new()
            session.update(self.session)
            request.update(Request())
            self.db_session = self.manager.session_maker()
            self.db_session.begin()
            db.update(self.db_session)
            redis.update(self.manager.redis)
            is_active.update(True)
            if self.manager.change_interceptor:
                self.manager.change_interceptor.start_record()
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.manager.web_session_man.disconnect(self.session, self.token)
            if not self.manager.auto_commit:
                return
            if not any((exc_val, exc_tb, exc_type)):
                await self.db_session.commit()
                if self.manager.change_interceptor:
                    self.manager.change_interceptor.end_transaction()
            else:
                await self.db_session.rollback()
            is_active.update(False)

    def __init__(self, session_maker: Callable,
                 redis_connection: Redis | str = 'redis://localhost:6379/0',
                 auto_commit: bool = False, trace_changes: bool = False, change_call_back: FunctionType = None):
        from .cache import setup_cache
        setup_cache(redis_connection=redis_connection)
        self.auto_commit = auto_commit
        self.session_maker = session_maker
        self.redis = Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection
        self.web_session_man = RedisSessionManager(redis_connection)
        if change_call_back or trace_changes:
            from .interceptors import ChangeInterceptor
            self.change_interceptor = ChangeInterceptor(change_call_back, request=request)
        else:
            self.change_interceptor = None

    def __call__(self, token: str | None = None):
        return self.Context(self, token)

    async def destroy(self, token: str):
        await self.web_session_man.destroy(token)

class ContextProxy:
    def __init__(self, name: str):
        self.__dict__['name'] = name
        self.__dict__['__var'] = ContextVar(name)

    def update(self, obj: object) -> None:
        self.__dict__['__var'].set(obj)

    def __getattr__(self, item: str):
        return getattr(self.__dict__['__var'].get(), item, None)

    def __setattr__(self, key, value):
        setattr(self.__dict__['__var'].get(), key, value)

    def __getitem__(self, item):
        return self.__dict__['__var'].get()[item]

    def __setitem__(self, key, value):
        self.__dict__['__var'].get()[key] = value

session = ContextProxy('session')
request = ContextProxy('request')
db = ContextProxy('db_session')
redis = ContextProxy('redis')
is_active = ContextProxy('is_active')