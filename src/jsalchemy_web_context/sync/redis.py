from uuid import uuid4

import redis
from pickle import loads, dumps, UnpicklingError

from .base import Storage, SessionManager
from ..exceptions import SessionNotFound


class RedisSessionManager(SessionManager):

    def __init__(self, redis_connection: redis.Redis | str = 'redis://localhost:6379/0',
                 key: str='session',
                 duration: int = 3600):
        self.connection = redis.Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection
        self.key = key
        self.duration = duration
        self.session_format = "{self.key}:{token}"

    def connect(self, token):
        raw = self.connection.get(self.session_format.format(token=token, self=self))
        if raw is None:
            raise SessionNotFound(token)
        try:
            self.connection.expire(self.session_format.format(token=token, self=self), self.duration)
            return Storage(loads(raw))
        except UnpicklingError as exc:
            raise SessionNotFound('Session corrupted') from exc

    def disconnect(self, session: Storage, token: str):
        self.connection.set(self.session_format.format(self=self, token=token),
                            session.dumps(), ex=self.duration)

    def new(self, token:str = None):
        """Create a new session for the given `token`."""
        token = token or str(uuid4())
        while self.connection.keys(self.session_format.format(self=self, token=token)):
            token = str(uuid4())
        self.connection.set(self.session_format.format(self=self, token=token), dumps({}))
        return token, Storage({})

    def destroy(self, token):
        """Destroy the session from Redis."""
        return bool(self.connection.delete(self.session_format.format(self=self, token=token)))
