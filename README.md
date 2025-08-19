# SQLAlchemy-web-context

SQLAlchemy-web-context is a package that provides an asynchronous context manager for SQLAlchemy with user sessions and
a connection to Redis.

## Key Concepts

- Async context manager for SQLAlchemy
- User sessions with Redis
- Connection to Redis

## Example usages

### Configuration

```python
from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm. import sessionmaker

from jsalchemy_web_context import ContextManager

async_engine = create_async_engine("postgresql+asyncpg://user:password@localhost:5432/db")

async_session_factory = sessionmaker(async_engine, expire_on_commit=False)

context_manager = ContextManager(
    session_maker=sessionmaker(async_engine, expire_on_commit=False),
    redis_connection='redis://localhost:6379/0')
```

The configuration in the example above makes available the `session`, the `request` and the `db` proxies pointing to the
current session, the current request and the current db session respectively.

### Usage

```python
# appending this from the above example

from jsalchemy_web_context import db, session, request
from sqlalchemy import select


async def main():
    async with context_manager(token='token'):
        for item in await session.query(Item).all():
            request.my_attribute = 'my value'  # this object is going to be available in until the context is open
            session.my_attribute = 'my value'  # this object will be stored in the session in Redis 
            ## and available in the same context using the same context
            my_items = await db.execute(
                select(Item)).all()  # db generated when you enter the context and committed when you leave

            db.add(Item(name='item'))
```

The example above shows how you can use the context manager to store the session in Redis and access it in the same
context.
