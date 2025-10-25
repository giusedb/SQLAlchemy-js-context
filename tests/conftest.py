import asyncio

import pytest
import pytest_asyncio
from fakeredis.aioredis import FakeRedis
from sqlalchemy import Column, Integer, ForeignKey, Table
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, relationship
from sqlalchemy.testing.schema import mapped_column


@pytest.fixture
def engine():
    """Connect SQLAlchemy to SQLite in-memory database and return the engine."""
    from sqlalchemy.ext.asyncio import create_async_engine

    return create_async_engine("sqlite+aiosqlite:///:memory:", )

@pytest.fixture
def session_maker(engine):
    """Connect SQLAlchemy to SQLLite in-memory database and return the sessionmager() function."""
    from sqlalchemy.ext.asyncio import async_sessionmaker

    return async_sessionmaker(engine, expire_on_commit=False)

@pytest.fixture
def item(session_maker, engine):
    """Return the Item model."""
    from sqlalchemy.orm import declarative_base, Mapped
    from sqlalchemy import Column, Integer

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
def context(session_maker):
    """Return a ContextManager instance."""
    from jsalchemy_web_context.manager import ContextManager
    return ContextManager(session_maker, FakeRedis.from_url('redis://localhost:6379/0'), auto_commit=True)


# @pytest.fixture
# def context(context_manager):
#     """Creates a context to be user withing an async with bloc."""
#     return context_manager()

@pytest.fixture
def Base():
    """Create the base model"""
    class Base(AsyncAttrs, DeclarativeBase):
        pass

    return Base

@pytest.fixture
def cls_models(Base):
    """Return a change call back function."""
    class Container(Base):
        __tablename__ = 'container'

        id: Mapped[int] = Column(Integer, primary_key=True, nullable=True)
        name: Mapped[str]

        def __repr__(self):
            return f"<Container {self.name}>"

    class Item(Base):
        __tablename__ = 'item'

        id: Mapped[int] = mapped_column(Integer, primary_key=True)
        name: Mapped[str]
        container_id: Mapped[int] = mapped_column(Integer, ForeignKey('container.id'))
        container: Mapped[Container] = relationship('Container', back_populates='items')

        def __repr__(self):
            return f"<Item {self.name}>"

    class OtherItem(Base):
        __tablename__ = 'other_item'

        id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=True)
        name: Mapped[str] = mapped_column('title')
        container_id: Mapped[int] = mapped_column(Integer, ForeignKey('container.id'))
        container: Mapped[Container] = relationship('Container', back_populates='other_items')

        def __repr__(self):
            return f"<OtherItem {self.name}>"

    item_other = Table('item_other', Base.metadata,
        Column('item_id', Integer, ForeignKey('item.id')),
        Column('other_item_id', Integer, ForeignKey('other_item.id'))
    )

    Container.other_items = relationship('OtherItem', back_populates='container')
    Container.items = relationship('Item', back_populates='container')
    Item.other_items = relationship('OtherItem', secondary=item_other, back_populates='items')
    OtherItem.items = relationship('Item', secondary=item_other, back_populates='other_items')

    return Container, Item, OtherItem

@pytest.fixture
def create_tables(Base, engine):
    async def define_tables():
        """Define the tables."""
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    return define_tables


@pytest_asyncio.fixture
async def models(cls_models, create_tables):
    await create_tables()
    return cls_models

## ----- RUN Sync ----

@pytest.fixture
def sync_engine():
    """Connect SQLAlchemy to SQLite in-memory database and return the engine."""
    from sqlalchemy import create_engine

    return create_engine("sqlite:///:memory:", )

@pytest.fixture
def sync_session_maker(sync_engine):
    """Connect SQLAlchemy to SQLLite in-memory database and return the sessionmager() function."""
    from sqlalchemy.orm import sessionmaker

    return sessionmaker(sync_engine, expire_on_commit=False)

@pytest.fixture
def sync_item(sync_session_maker, sync_engine):
    """Return the Item model."""
    from sqlalchemy.orm import declarative_base, Mapped
    from sqlalchemy import Column, Integer

    Base = declarative_base()

    class Item(Base):
        __tablename__ = 'item'

        id: Mapped[int] = Column(Integer, primary_key=True, nullable=True)
        name: Mapped[str]

        def __repr__(self):
            return f"<Item {self.name}>"

    def init():
        with sync_engine.begin() as connection:
            Base.metadata.create_all(connection)

    init()

    return Item

@pytest.fixture
def sync_context_manager(sync_session_maker):
    """Return a ContextManager instance."""
    from jsalchemy_web_context.sync.manager import ContextManager
    from fakeredis import FakeRedis
    return ContextManager(sync_session_maker, FakeRedis.from_url('redis://localhost:6379/0'))


@pytest.fixture
def sync_context(context_manager):
    """Creates a context to be user withing an async with bloc."""
    return sync_context_manager()
