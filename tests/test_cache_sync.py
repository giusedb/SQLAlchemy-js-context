from itertools import permutations

from typing_extensions import NamedTuple

from jsalchemy_web_context.sync.cache import request_cache, redis_cache, setup_cache

# --- REQUEST CACHING ---

def test_cache_request(sync_context):
    count = 0

    @request_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        ret = mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

def test_cache_request_with_objects(sync_context):
    count = 0
    class Foo(NamedTuple):
        id: int
        value: int
        name: str

    foo = Foo(2, 20, 'b')

    @request_cache('a.id', 'b.id')
    def mysum(a: Foo, b:Foo=foo, c=0):
        nonlocal count
        count += 1
        return a.value + b.value + c

    with sync_context():
        ret = mysum(Foo(1, 100, 'a'), Foo(2, 200, 'b'))
        assert ret == 300
        assert count == 1

        ret = mysum(Foo(1, 300, '-'), Foo(2, 20, '-'))
        assert ret == 300
        assert count == 1

        ret = mysum(Foo(1, 30, 'a'))
        assert ret == 300   # Correct due to cache
        assert count == 1

        ret = mysum(Foo(2, 30, 'a'))
        assert ret == 50
        assert count == 2

def test_cache_request_discard_all(sync_context):
    count = 0

    @request_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        ret = mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 3)
        assert ret == 9
        assert count == 1

        mysum.discard_all()

        ret = mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

def test_cache_request_discard(sync_context):
    count = 0

    @request_cache('b', 'a')
    def mysub(a=3, b=2, c=0):
        nonlocal count
        count += 1
        return a - b - c


    @request_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        mysub.discard_all()
        mysum.discard_all()

        ret = mysum(1)
        assert ret == 3
        assert count == 1

        ret = mysum(1)
        assert ret == 3
        assert count == 1

        mysum.discard(1)

        ret = mysum(1)
        assert ret == 3
        assert count == 2

        count = 0
        mysub(1)
        assert count == 1

        for x, y in permutations(range(1, 6), 2):
            ret = mysub(x, y)
            assert ret == x - y
        assert count == 20

        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 20

        mysub.discard(a=1)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 4

        mysub.discard(b=2)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 4

        mysub.discard(1, 2)
        mysub.discard(2, 1)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 2

# --- REDIS CACHING ---

def test_cache_redis(sync_context):
    count = 0

    @redis_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        mysum.discard_all()

        ret = mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

def test_cache_redis_with_objects(sync_context):
    count = 0
    class Foo(NamedTuple):
        id: int
        value: int
        name: str

    foo = Foo(2, 20, 'b')

    @redis_cache('a.id', 'b.id')
    def mysum(a: Foo, b:Foo=foo, c=0):
        nonlocal count
        count += 1
        return a.value + b.value + c

    with sync_context():
        ret = mysum(Foo(1, 100, 'a'), Foo(2, 200, 'b'))
        assert ret == 300
        assert count == 1

        ret = mysum(Foo(1, 300, '-'), Foo(2, 20, '-'))
        assert ret == 300
        assert count == 1

        ret = mysum(Foo(1, 30, 'a'))
        assert ret == 300   # Correct due to cache
        assert count == 1

        ret = mysum(Foo(2, 30, 'a'))
        assert ret == 50
        assert count == 2

def test_cache_redis_discard_all(sync_context):
    count = 0

    @redis_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        mysum.discard_all()

        ret = mysum(3, 3, 3)
        assert ret == 9
        assert count == 1

        ret = mysum(3, 3)
        assert ret == 9
        assert count == 1

        mysum.discard_all()

        ret = mysum(3, 4, 2)
        assert ret == 9
        assert count == 2

def test_cache_redis_discard(sync_context):
    count = 0

    @redis_cache('b', 'a')
    def mysub(a=3, b=2, c=0):
        nonlocal count
        count += 1
        return a - b - c


    @redis_cache('a', 'b')
    def mysum(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        mysum.discard_all()
        mysub.discard_all()

        ret = mysum(1)
        assert ret == 3
        assert count == 1

        ret = mysum(1)
        assert ret == 3
        assert count == 1

        mysum.discard(1)

        ret = mysum(1)
        assert ret == 3
        assert count == 2

        count = 0
        mysub(1)
        assert count == 1

        for x, y in permutations(range(1, 6), 2):
            ret = mysub(x, y)
            assert ret == x - y
        assert count == 20

        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 20

        mysub.discard(a=1)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 4

        mysub.discard(b=2)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 4

        mysub.discard(1, 2)
        mysub.discard(2, 1)

        count = 0
        for args in permutations(range(1, 6), 2):
            mysub(*args)
        assert count == 2

def test_cache_redis_ttl(sync_context):
    import time
    count = 0
    setup_cache(default_ttl=200)

    @redis_cache('a', 'b', ttl=1)
    def mysum2(a, b=2, c=0):
        nonlocal count
        count += 1
        return a + b + c

    with sync_context():
        mysum2.discard_all()

        ret = mysum2(1)
        assert ret == 3
        assert count == 1

        ret = mysum2(1)
        assert ret == 3
        assert count == 1

        time.sleep(2.3)

        ret = mysum2(1)
        assert ret == 3
        assert count == 2


