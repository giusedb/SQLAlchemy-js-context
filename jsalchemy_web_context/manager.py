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
            return self

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            await self.manager.web_session_man.disconnect(self.session, self.token)
            if not any((exc_val, exc_tb, exc_type)):
                await self.db_session.commit()
            else:
                await self.db_session.rollback()

    def __init__(self, session_maker: Callable,
                 redis_connection: Redis | str = 'redis://localhost:6379/0'):
        self.session_maker = session_maker
        self.web_session_man = RedisSessionManager(redis_connection)

    def __call__(self, token: str | None = None):
        return self.Context(self, token)


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
