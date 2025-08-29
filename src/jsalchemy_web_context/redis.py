from uuid import uuid4

from redis.asyncio import Redis
from pickle import loads, dumps, UnpicklingError

from .base import Storage, SessionManager
from .exceptions import SessionNotFound


class RedisSessionManager(SessionManager):

    def __init__(self, redis_connection: Redis | str = 'redis://localhost:6379/0',
                 key: str='session',
                 duration: int = 3600):
        self.connection = Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection
        self.key = key
        self.duration = duration
        self.session_format = "{self.key}:{token}"

    async def connect(self, token):
        raw = await self.connection.get(self.session_format.format(token=token, self=self))
        if raw is None:
            raise SessionNotFound(token)
        try:
            await self.connection.expire(self.session_format.format(token=token, self=self), self.duration)
            return Storage(loads(raw))
        except UnpicklingError as exc:
            raise SessionNotFound('Session corrupted') from exc

    async def disconnect(self, session: Storage, token: str):
        await self.connection.set(self.session_format.format(self=self, token=token),
                                  session.dumps(), ex=self.duration)

    async def new(self, token:str = None):
        """Create a new session for the given `token`."""
        token = token or str(uuid4())
        while await self.connection.keys(self.session_format.format(self=self, token=token)):
            token = str(uuid4())
        await self.connection.set(self.session_format.format(self=self, token=token), dumps({}))
        return token, Storage({})

    async def destroy(self, token):
        """Destroy the session from Redis."""
        return bool(await self.connection.delete(self.session_format.format(self=self, token=token)))
