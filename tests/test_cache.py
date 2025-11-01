import asyncio
from itertools import permutations

import pytest
from typing_extensions import NamedTuple

from jsalchemy_web_context.cache import request_cache, redis_cache, setup_cache

# --- REQUEST CACHING ---

@pytest.mark.asyncio
async def test_cache_request(context):
    count = 0

    @request_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():
        ret = await mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

@pytest.mark.asyncio
async def test_cache_request_with_objects(context):
    count = 0
    class Foo(NamedTuple):
        id: int
        value: int
        name: str

    foo = Foo(2, 20, 'b')

    @request_cache('a.id', 'b.id')
    async def mysum(a: Foo, b:Foo=foo, c=0):
        nonlocal count
        count += 1
        return a.value + b.value + c

    async with context():
        ret = await mysum(Foo(1, 100, 'a'), Foo(2, 200, 'b'))
        assert ret == 300
        assert count == 1

        ret = await mysum(Foo(1, 300, '-'), Foo(2, 20, '-'))
        assert ret == 300
        assert count == 1

        ret = await mysum(Foo(1, 30, 'a'))
        assert ret == 300   # Correct due to cache
        assert count == 1

        ret = await mysum(Foo(2, 30, 'a'))
        assert ret == 50
        assert count == 2

@pytest.mark.asyncio
async def test_cache_request_discard_all(context):
    count = 0

    @request_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():
        ret = await mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 3)
        assert ret == 9
        assert count == 1

        await mysum.discard_all()

        ret = await mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

@pytest.mark.asyncio
async def test_cache_request_discard(context):
    count = 0

    @request_cache('b', 'a')
    async def mysub(a=3, b=2, c=0):
        nonlocal count
        count += 1
        return a - b - c


    @request_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():

        ret = await mysum(1)
        assert ret == 3
        assert count == 1

        ret = await mysum(1)
        assert ret == 3
        assert count == 1

        await mysum.discard(1)

        ret = await mysum(1)
        assert ret == 3
        assert count == 2

        count = 0
        await mysub(1)
        assert count == 1

        for x, y in permutations(range(1, 6), 2):
            ret = await mysub(x, y)
            assert ret == x - y
        assert count == 20

        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 20

        await mysub.discard(a=1)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 4

        await mysub.discard(b=2)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 4

        await mysub.discard(1, 2)
        await mysub.discard(2, 1)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 2

# --- REDIS CACHING ---

@pytest.mark.asyncio
async def test_cache_redis(context):
    count = 0

    @redis_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():
        ret = await mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

@pytest.mark.asyncio
async def test_cache_redis_with_objects(context):
    count = 0
    class Foo(NamedTuple):
        id: int
        value: int
        name: str

    foo = Foo(2, 20, 'b')

    @redis_cache('a.id', 'b.id')
    async def mysum(a: Foo, b:Foo=foo, c=0):
        nonlocal count
        count += 1
        return a.value + b.value + c

    async with context():
        ret = await mysum(Foo(1, 100, 'a'), Foo(2, 200, 'b'))
        assert ret == 300
        assert count == 1

        ret = await mysum(Foo(1, 300, '-'), Foo(2, 20, '-'))
        assert ret == 300
        assert count == 1

        ret = await mysum(Foo(1, 30, 'a'))
        assert ret == 300   # Correct due to cache
        assert count == 1

        ret = await mysum(Foo(2, 30, 'a'))
        assert ret == 50
        assert count == 2

@pytest.mark.asyncio
async def test_cache_redis_discard_all(context):
    count = 0

    @redis_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():
        await mysum.discard_all()

        ret = await mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = await mysum(3, 3)
        assert ret == 9
        assert count == 1

        await mysum.discard_all()

        ret = await mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

@pytest.mark.asyncio
async def test_cache_redis_discard(context):
    count = 0

    @redis_cache('b', 'a')
    async def mysub(a=3, b=2, c=0):
        nonlocal count
        count += 1
        return a - b - c


    @redis_cache('a', 'b')
    async def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():

        ret = await mysum(1)
        assert ret == 3
        assert count == 1

        ret = await mysum(1)
        assert ret == 3
        assert count == 1

        await mysum.discard(1)

        ret = await mysum(1)
        assert ret == 3
        assert count == 2

        count = 0
        await mysub(1)
        assert count == 1

        for x, y in permutations(range(1, 6), 2):
            ret = await mysub(x, y)
            assert ret == x - y
        assert count == 20

        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 20

        await mysub.discard(a=1)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 4

        await mysub.discard(b=2)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 4

        await mysub.discard(1, 2)
        await mysub.discard(2, 1)

        count = 0
        for args in permutations(range(1, 6), 2):
            await mysub(*args)
        assert count == 2

@pytest.mark.asyncio
async def test_cache_redis_ttl(context):
    count = 0
    setup_cache(default_ttl=200)

    @redis_cache('a', 'b', ttl=1)
    async def mysum2(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    async with context():

        ret = await mysum2(1)
        assert ret == 3
        assert count == 1

        ret = await mysum2(1)
        assert ret == 3
        assert count == 1

        await asyncio.sleep(1.01)

        ret = await mysum2(1)
        assert ret == 3
        assert count == 2


