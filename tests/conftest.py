import asyncio

import pytest
from fakeredis.aioredis import FakeRedis


@pytest.fixture
def engine():
    """Connect SQLAlchemy to SQLite in-memory database and return the engine."""
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    return create_async_engine("sqlite+aiosqlite:///:memory:", )

@pytest.fixture
def session_maker(engine):
    """Connect SQLAlchemy to SQLLite in-memory database and return the sessionmager() function."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    return async_sessionmaker(engine, expire_on_commit=False)

@pytest.fixture
def item(session_maker, engine):
    from sqlalchemy.orm import declarative_base, Mapped
    from sqlalchemy import Column, Integer, String

    Base = declarative_base()

    class Item(Base):
        __tablename__ = 'item'

        id: Mapped[int] = Column(Integer, primary_key=True, nullable=True)
        name: Mapped[str]

        def __repr__(self):
            return f"<Item {self.name}>"

    async def init():
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    asyncio.run(init())

    return Item

@pytest.fixture
def context_manager(session_maker):
    from jsalchemy_web_context.manager import ContextManager
    return ContextManager(session_maker, FakeRedis.from_url('redis://localhost:6379/0'))