import inspect
import pickle
import re
from functools import wraps

import fakeredis

from jsalchemy_web_context import request

CACHE_TTL = 3600
CACHE = fakeredis.aioredis.FakeRedis()
SEPARATOR = '::'

def setup_cache(default_ttl: int=None, redis_connection: str=None, separator: str=None):
    global CACHE_TTL
    global CACHE
    global SEPARATOR
    if default_ttl is not None:
        CACHE_TTL = default_ttl
    if redis_connection is not None:
        from redis.asyncio import Redis
        CACHE = Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection
    if separator is not None:
        SEPARATOR = separator

def memoize_one(func):
    cache = {}

    @wraps(func)
    def wrapper(arg):
        if arg not in cache:
            cache[arg] = func(arg)
        return cache[arg]
    return wrapper

def memoize_args(func):
    cache = {}

    @wraps(func)
    def wrapper(*args, **kwargs):
        if args not in cache:
            cache[args] = func(*args, **kwargs)
        return cache[args]
    return wrapper

def _get_attr_value(value, path):
    """
    Walk a dotted attribute chain on `value`.

    Example:  _get_attr_value(obj, 'address.zip') → obj.address.zip
    """
    for part in path.split('.'):
        value = getattr(value, part)
    return value

def _make_key(bound, attr_paths, separator, for_removal=False):
    """
    `bound` is a BoundArguments object (from inspect.signature.bind).
    Return a tuple that will be used as the cache key.
    """
    parts = []
    if for_removal:
        missing_args = {x: n for n, x in enumerate(x.split('.')[0] for x in attr_paths)
                        if x not in bound.arguments}
    else:
        missing_args = {}
    for path in attr_paths:
        param, _, rest = path.partition('.')
        if param in missing_args:
            parts.append(r'.*?')
            continue
        if not rest:
            val = bound.arguments[param]
        else:
            val = _get_attr_value(bound.arguments[param], rest)
        parts.append(str(val))
    if for_removal:
        skip = set(missing_args.values())
        return separator.join(x if n in skip else re.escape(x) for n, x in enumerate(parts))
    return separator.join(parts)

def sync_redis_cache(*attr_paths):

    def decorator(func):
        sig = inspect.signature(func)

        @wraps(func)
        def wrapper(*args, **kwargs):
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            key_tuple = _make_key(bound, attr_paths)
            # convert the tuple to a string that Redis can use
            key = ':'.join(map(str, key_tuple))

            cached = CACHE.get(key)
            if cached is not None:
                # Redis returns bytes – convert back to the original type
                return pickle.loads(cached)

            result = func(*args, **kwargs)
            CACHE.set(key, pickle.dumps(result))
            return result

        return wrapper
    return decorator

def request_cache(*attr_paths, separator=None):
    """
    Decorator that caches a coroutine’s return value in a dictionary
    stored on the current `request` object.

    * The cache is created as an attribute called `cache` on the request.
    * Each entry is stored under a key that is built from the supplied
      attribute paths (same algorithm as before).
    """


    def decorator(func):
        nonlocal separator
        sig = inspect.signature(func)
        cache_key = F"{func.__name__}"
        separator = separator or SEPARATOR

        def get_local_cache():
            cache = getattr(request, cache_key, None)
            if cache is None:
                cache = {}
                setattr(request, cache_key, cache)
            return cache

        @wraps(func)
        async def wrapper(*args, **kwargs):
            cache = get_local_cache()

            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            key = _make_key(bound, attr_paths, separator)

            if key in cache:
                return cache[key]

            result = await func(*args, **kwargs)
            cache[key] = result
            return result

        def discard_all():
            get_local_cache().clear()

        def discard(*d_args, **d_kwargs):
            nonlocal separator
            if not hasattr(request, cache_key):
                return
            cache = get_local_cache()

            # Build a signature that does **not** contain the `request` param
            discard_sig = sig.replace(parameters=list(sig.parameters.values()))

            try:
                discard_bound = discard_sig.bind(*d_args, **d_kwargs)
            except TypeError:
                return discard_all()

            attr_match = re.compile(_make_key(discard_bound, attr_paths, separator, for_removal=True))

            to_remove = {x for x in cache if attr_match.match(x)}
            for key in to_remove:
                del cache[key]

        wrapper.discard_all = discard_all
        wrapper.discard = discard

        return wrapper
    return decorator

def redis_cache(*attr_paths, ttl=CACHE_TTL, separator=None):
    """
    Decorator that caches a coroutine’s return value in a Redis hash.

    * The hash name is the wrapped function’s __name__.
    * Each cache entry is stored under a field that is built from
      the supplied attribute paths.
    """
    separator = separator or SEPARATOR

    def decorator(func):
        sig = inspect.signature(func)
        cache_key = F"{func.__name__}"

        @wraps(func)
        async def wrapper(*args, **kwargs):
            nonlocal separator
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            key = _make_key(bound, attr_paths, separator)

            blob = await CACHE.hget(cache_key, key)
            if blob is not None:
                return pickle.loads(blob)

            result = await func(*args, **kwargs)
            await CACHE.hsetex(cache_key, key, pickle.dumps(result), ex=ttl)
            return result

        async def discard_all():
            await CACHE.delete(cache_key)

        async def discard(*d_args, **d_kwargs):
            nonlocal separator
            if not hasattr(request, cache_key):
                return
            # Build a signature that does **not** contain the `request` param
            discard_sig = sig.replace(parameters=list(sig.parameters.values()))

            try:
                discard_bound = discard_sig.bind(*d_args, **d_kwargs)
            except TypeError:
                return discard_all()

            attr_match = re.compile(_make_key(discard_bound, attr_paths, separator, for_removal=True))
            cached = await CACHE.hkeys(cache_key)

            to_remove = {x for x in cached if attr_match.match(x.decode('utf-8'))}
            if to_remove:
                await CACHE.hdel(cache_key, *to_remove)

        wrapper.discard_all = discard_all
        wrapper.discard = discard

        return wrapper

    return decorator

