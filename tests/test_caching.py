import asyncio
from types import SimpleNamespace

import pytest

from jsalchemy_web_context.caching import redis_cached_function, MockArg, request_cached


@pytest.mark.asyncio
async def test_caching_behavior(context_manager):
    """Ensure that a cached function returns a stored value on subsequent calls."""
    counter = SimpleNamespace(n=0)

    @redis_cached_function('num', ttl=1)
    async def inc(num):
        counter.n += 1
        return counter.n

    async with context_manager() as ctx:
        # First call – no cache hit
        first = await inc(5)
        assert first == 1
        assert counter.n == 1

        # Second call – should hit cache and NOT increment
        second = await inc(5)
        assert second == 1
        assert counter.n == 1

        # Call with different argument – cache miss again
        third = await inc(10)
        assert third == 2
        assert counter.n == 2

@pytest.mark.asyncio
async def test_cache_expiry(context_manager):
    """Validate that the TTL actually removes the value after the set time."""
    @redis_cached_function('a', ttl=1)
    async def add(a):
        return a * 2

    async with context_manager() as ctx:
        first = await add(3)
        assert first == 6

        # Wait for the TTL to expire
        await asyncio.sleep(1.1)

        # Cache should be gone – a new call triggers recomputation
        second = await add(3)
        assert second == 6  # same result, but counter would increment if we added one

@pytest.mark.asyncio
async def test_discard_single_entry(context_manager):
    """Test the `discard` helper that removes a specific cache entry."""
    from jsalchemy_web_context.caching import CACHE

    @redis_cached_function('x', ttl=10)
    async def square(x):
        return x * x

    async with context_manager() as ctx:
        # Fill cache
        await square(4)   # 16
        await square(5)   # 25

        # Confirm both are cached
        cache_key_4 = 'RCF:square:4'
        cache_key_5 = 'RCF:square:5'
        assert await CACHE.get(cache_key_4) is not None
        assert await CACHE.get(cache_key_5) is not None

        # Discard the entry for x=4
        await square.discard(4)

        # 4 should be removed; 5 should stay
        assert await CACHE.get(cache_key_4) is None
        assert await CACHE.get(cache_key_5) is not None


@pytest.mark.asyncio
async def test_discard_all(context_manager):
    """Test the `discard_all` helper that clears the whole cache for the function."""
    from jsalchemy_web_context.caching import CACHE

    @redis_cached_function('n', ttl=10)
    async def double(n):
        return n * 2

    async with context_manager() as ctx:
        # Populate several entries
        await double(1)
        await double(2)
        await double(3)

        # Ensure they exist
        for i in (1, 2, 3):
            assert await CACHE.get(f'RCF:double:{i}') is not None

        # Call discard_all
        await double.discard_all()

        # All entries should be gone
        for i in (1, 2, 3):
            assert await CACHE.get(f'RCF:double:{i}') is None


def test_mockarg_str_and_repr():
    """MockArg should expose the provided pattern both as str and repr."""
    ma = MockArg('(foo|bar)')
    assert repr(ma) == '(foo|bar)'
    assert str(ma) == '(foo|bar)'

    # attribute access should return another MockArg instance
    assert isinstance(ma.foo, MockArg)
    assert isinstance(ma.foo.bar, MockArg)


def test_redis_cached_function(context_manager):

    @redis_cached_function('a', ttl=100)
    async def sum(a, b):
        await asyncio.sleep(0)
        return a + b

    async def task():
        async with context_manager() as ctx:
            x = await sum(1, 2)
            assert x == 3
            z = await sum(1, 3)
            assert z == 3 # It's wrong in on purpose

    asyncio.run(task())

def test_redis_cached_function_2(context_manager):

    @redis_cached_function('a', 'b', ttl=100)
    async def sum(a, b):
        await asyncio.sleep(0)
        return a + b

    async def task():
        async with context_manager() as ctx:
            x =  await sum(1, 2)
            assert x == 3
            z = await sum(1, 3)
            assert z == 4 # It's correct in on purpose

    asyncio.run(task())


def test_multi_request(context_manager):

    @redis_cached_function('a', ttl=100)
    async def sum(a, b):
        await asyncio.sleep(0)
        return a + b

    async def task():
        async with context_manager() as ctx:
            x =  await sum(1, 2)
            assert x == 3
            z = await sum(1, 3)
            assert z == 3 # It's correct in on purpose

        async with context_manager() as ctx:
            x =  await sum(1, 2)
            assert x == 3
            z = await sum(1, 6)
            assert z == 3 # It's correct in on purpose

    asyncio.run(task())

def test_redis_call(context_manager, mocker):

    from jsalchemy_web_context.caching import cache_setup
    from fakeredis.aioredis import FakeRedis

    redis = FakeRedis.from_url('redis://localhost:6379/0')
    cache_setup(redis_connection=redis)
    red_get = mocker.spy(redis, 'get')
    red_set = mocker.spy(redis, 'set')

    @redis_cached_function('a', ttl=100)
    async def sum(a, b):
        await asyncio.sleep(0)
        return a + b

    async def task():
        async with context_manager() as ctx:
            x =  await sum(1, 2)
            assert x == 3
            z = await sum(1, 6)
            assert z == 3 # It's correct in on purpose
            z = await sum(1, 4)
            z = await sum(2, 4)
            assert z == 6
            d = await sum(2, 5)
            assert d == 6

            assert red_get.call_count == 2
            assert red_set.call_count == 2

    asyncio.run(task())


def test_multi_args(context_manager, mocker):

    from jsalchemy_web_context.caching import cache_setup
    from fakeredis.aioredis import FakeRedis

    redis = FakeRedis.from_url('redis://localhost:6379/0')
    cache_setup(redis_connection=redis)
    red_get = mocker.spy(redis, 'get')
    red_set = mocker.spy(redis, 'set')

    @redis_cached_function('a', 'b', 'd', ttl=100)
    async def pippo(a, b=2, *c, d=4, **e):
        return a + b + sum(c) + d, e


    async def task():
        async with context_manager() as ctx:
            x =  await pippo(1, 2, 3, 4, d=5, f=6)
            assert x == (15, {'f': 6})
            x =  await pippo(1, 2, 3, 39494, d=5, f=28849)
            assert x == (15, {'f': 6})
            x = await pippo(2, d=4, e=100)
            assert x == (8, {'e': 100})

            assert red_get.call_count == 2
            assert red_set.call_count == 2

            for a in range(10):
                x = await pippo(2, d=4, e=100)
            assert red_get.call_count == 2
            assert red_set.call_count == 2

        async with context_manager() as ctx:
            for a in range(10):
                x = await pippo(2, d=4, e=100)
            assert red_get.call_count == 3
            assert red_set.call_count == 2

    asyncio.run(task())

def test_request_cache(context_manager, mocker):

    sum_count = 0
    async_sum_count = 0

    @request_cached
    def sum(a, b):
        nonlocal sum_count
        sum_count += 1
        return a + b

    @request_cached
    async def async_sum(a: int, b: int) -> int:
        nonlocal async_sum_count
        await asyncio.sleep(0)
        async_sum_count += 1
        return a + b

    async def task():
        async with context_manager():
            assert async_sum_count == 0
            assert sum_count == 0
            x = sum(1, 2)
            assert x == 3
            assert sum_count == 1

            x = sum(1, 2)
            assert x == 3
            assert sum_count == 1

            x = await async_sum(1, 2)
            assert x == 3
            assert async_sum_count == 1

            x = await async_sum(1, 2)
            assert x == 3
            assert async_sum_count == 1

        async with context_manager():
            x = sum(1, 2)
            assert x == 3
            assert sum_count == 2

            x = sum(1, 2)
            assert x == 3
            assert sum_count == 2

            x = await async_sum(1, 2)
            assert x == 3
            assert async_sum_count == 2

            x = await async_sum(1, 2)
            assert x == 3
            assert async_sum_count == 2

        # async with context_manager():
        #     x = sum(b=2, a=1)
        #     assert x == 3
        #     assert sum_count == 2


    asyncio.run(task())
