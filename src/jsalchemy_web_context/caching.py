import asyncio
import inspect
import re
from functools import wraps
from pickle import dumps
from pickle import loads
from types import FunctionType
from types import MethodType
from redis.asyncio import Redis
import fakeredis

from . import request

RE_CLASS_NAME = re.compile(r'_?[A-Z]\w+')
CACHE_TTL = 3600
CACHE = fakeredis.aioredis.FakeRedis()

def cache_setup(default_ttl=CACHE_TTL, redis_connection=None):
    global CACHE_TTL, CACHE
    CACHE_TTL = default_ttl
    if redis_connection is not None:
        CACHE = Redis.from_url(redis_connection) if isinstance(redis_connection, str) else redis_connection


class MockArg:
    """Mock arguments of `redis_cached_function`."""

    def __init__(self, key='(.*?)'):
        self.key = key

    def __getattribute__(self, item):
        if item not in {'__init__', '__repr__', 'key', '__str__'}:
            return MockArg(self.key)
        return object.__getattribute__(self, item)

    def __repr__(self):
        return self.key

    __str__ = __repr__


class ArgumentParser:
    def __init__(self, func, args, use_redis_inner_dicts=False):
        self.use_redis_dicts = use_redis_inner_dicts
        self.args = args
        self.func_args = func_args = inspect.getfullargspec(func)
        self.dict_args = {arg: None for arg in func_args.args}
        if defaults := func_args.defaults:
            self.dict_args |= zip(func_args.args[-len(defaults):], defaults)
        self.build_keys(func)

    def build_keys(self, func, cls=None):
        self.key_format = ';'.join(f'{{{arg}}}' for arg in self.args)
        func_name = f'{cls.__name__}.{func.__name__}' if cls else func.__name__
        if not self.use_redis_dicts:
            self.key_format = f'RCF:{func_name}:{self.key_format}'
        self.build_key = self.key_format.format

    def key(self, args, kwargs):
        return self.build_key(**self.key_dict(args, kwargs))

    def key_dict(self, args, kwargs):
        current_args = self.dict_args.copy()
        current_args.update(zip(self.func_args.args, args))
        current_args.update(kwargs)
        return current_args

    def pattern(self, args, kwargs):
        key_dict = self.key_dict(args, kwargs)
        args = {key: MockArg('*') if value is None else value for key, value in key_dict.items()}
        return self.key_format.format(**args)

    def regex(self, args, kwargs):
        key_dict = self.key_dict(args, kwargs)
        args = {key: MockArg('(.*?)') if value is None else value for key, value in key_dict.items()}
        return re.compile(self.key_format.format(**args))


def redis_cached_function(*attr_names, **kwargs):
    """Decorate a redis cachable-result function.

    Example 1:
        @redis_cached_function('user.id')
        def my_userbound_function(user):
            ....

    Example 2:
        @redis_cached_function('registration.registration_form_id',
                               'registration.entity.__class__.__name__',
                               'registration.entity.id')
        def my_complex_bound_function(registration):
            ....
        the above makes the cache bound the registration form and the entity represented even tough those are not in the
        function signature.

    :param ttl: cache ttl expressed in no of seconds
    :param attr_names: is the list attribute you want to use to bind the cache to.
    :type attr_names: [str]
    """
    def decorator(func):
        redis_name = f'RCF:{func.__name__}:{{}}'
        parser = ArgumentParser(func, attr_names, kwargs.get('redis_dict', False))
        key_getter = parser.key
        cache_ttl = kwargs.get('ttl', CACHE_TTL)

        def get_local_cache():
            if not request:
                return {}
            g_key = f'RCF_cache_{func.__name__}'
            if getattr(request, g_key, None) is None:
                setattr(request, g_key, {})
            return getattr(request, g_key)

        @wraps(func)
        async def decorated_func(*args, **kwargs):
            args_key = key_getter(args, kwargs)
            g_cache = get_local_cache()
            if args_key in g_cache:
                return g_cache[args_key]
            blob = await (CACHE.hget(redis_name, args_key) if parser.use_redis_dicts else CACHE.get(args_key))
            if blob is None:
                ret = await func(*args, **kwargs)
                blob = dumps(ret)
                if parser.use_redis_dicts:
                    await CACHE.hset(redis_name, args_key, blob)
                else:
                    await CACHE.set(args_key, blob)
                await CACHE.expire(args_key, cache_ttl)
                g_cache[args_key] = ret
            else:
                ret = loads(blob)
                g_cache[args_key] = ret
            return ret

        async def discard(*args, **kwargs):
            g_cache = get_local_cache()
            if not all(parser.key_dict(args, kwargs).values()):
                if parser.use_redis_dicts:
                    redis_keys = await CACHE.hkeys(redis_name)
                    re_key = parser.regex(args, kwargs)
                    for key in redis_keys:
                        if re_key.match(key):
                            await CACHE.hdel(redis_name, key)
                else:
                    for redis_key in await CACHE.keys(parser.pattern(args, kwargs)):
                        await CACHE.delete(redis_key)
                re_key = parser.regex(args, kwargs)
                for key in set(g_cache):
                    if re_key.match(key):
                        g_cache.pop(key, None)

            else:
                arg_keys = key_getter(args, kwargs)
                g_cache.pop(arg_keys, None)
                if parser.use_redis_dicts:
                    await CACHE.hdel(redis_name, arg_keys)
                else:
                    await CACHE.delete(arg_keys)

        async def discard_all(self=None):
            g_local = get_local_cache()
            g_local.clear()
            if parser.use_redis_dicts:
                for key in await CACHE.keys(parser.regex([self], {})):
                    await CACHE.delete(key)
                await CACHE.delete(redis_name)
            else:
                for key in await CACHE.keys(parser.pattern([], {})):
                    await CACHE.delete(key)

        decorated_func.discard = discard
        decorated_func.discard_all = discard_all
        decorated_func.relink = parser.build_keys

        return decorated_func

    if attr_names and isinstance(attr_names[0], FunctionType):
        return decorator(attr_names[0])
    return decorator


def redis_cached_property(*attr_names, **kwargs):
    """Create cached property.

    you can specify the TTL by
    @redis_cached_property(ttl=300)
    """
    if attr_names:
        keyer = (f'self.{name}' for name in attr_names)
    else:
        keyer = ('self.__class__.__name__', )

    def decorate(func):
        return property(redis_cached_function(*keyer, **kwargs)(func))

    return decorate


def with_redis_cached(cls):
    """Decorate the class which makes use of `redis_cached_function` decorator."""
    iterator = ((attr_name, attr) for attr_name, attr in cls.__dict__.items() if isinstance(attr, FunctionType))
    for method_name, method in iterator:
        if not hasattr(method, 'discard'):
            continue
        method.discard = MethodType(method.discard, cls)
        method.discard_all = MethodType(method.discard_all, cls)
        method.relink(method, cls)

    iterator = tuple((attr_name, attr) for attr_name, attr in cls.__dict__.items() if isinstance(attr, property))
    for prop_name, prop in iterator:
        if hasattr(prop.fget, 'discard_all'):
            setattr(cls, f'discard_{prop_name}', prop.fget.discard_all)
            prop.fget.relink(prop.fget, cls)
    return cls


def request_cached(func):
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            """Decorate a request cachable-result function."""
            attr_name = f"RC_{func.__name__}"
            cache = getattr(request, attr_name, None)
            if cache is None:
                cache = {}
                setattr(request, attr_name, cache)
            if kwargs:
                raise ValueError('request_cache does not support kwargs')
            if args not in cache:
                cache[args] = await func(*args)
            return cache[args]
    else:
        @wraps(func)
        def wrapped(*args, **kwargs):
            """Decorate a request cachable-result function."""
            attr_name = f"RC_{func.__name__}"
            cache = getattr(request, attr_name, None)
            if cache is None:
                cache = {}
                setattr(request, attr_name, cache)
            if kwargs:
                raise ValueError('request_cache does not support kwargs')
            if args not in cache:
                cache[args] = func(*args)
            return cache[args]

    return wrapped


