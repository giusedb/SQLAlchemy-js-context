from asyncio import gather, run

from sqlalchemy import select
from jsalchemy_web_context import ContextManager, session, request, db
import asyncio


def test_proxy_dict_isolation():
    from jsalchemy_web_context.manager import ContextProxy

    prop = ContextProxy('property')
    elements = [{'foo': x} for x in range(100)]
    result = []

    def connect():
        prop.update(elements.pop())

    async def task():
        result.append(prop['foo'])
        await asyncio.sleep(0)
        prop['bar'] = []
        prop['bar'].append('-')
        await asyncio.sleep(0)
        result.append(''.join(prop['bar']))

    def prepare():
        connect()
        return asyncio.create_task(task())

    async def main():
        await asyncio.gather(*(prepare() for _ in range(10)))
        await asyncio.gather(*(prepare() for _ in range(10)))

    asyncio.run(main())
    max_dashes = max(map(len, filter(lambda x: type(x) is str and x.startswith('-'), result)))
    assert max_dashes == 1
    assert len(result) == 40

def test_context_segregation(context_manager):
    from jsalchemy_web_context import session, request


    async def request1(x):
        token = None
        async def req(y):
            nonlocal token
            async with context_manager(token) as ctx:
                assert bool(token) == bool(y)
                if token:
                    _ = session.foo == 'foobar', 'Session not connected'
                    assert token == ctx.token, "Session doesn't reconnect"
                else:
                    assert session.foo == None, 'Session not connected'
                    request.foo = 'bar'
                    session.foo = 'foobar'
                token = ctx.token

            async with context_manager(token) as ctx:
                assert session.foo == 'foobar', 'Session not connected'
                get = request.foo
                assert get == None
                request.foo = 'bar'
                assert request.foo == 'bar', 'Request not connected'
        await req(0)
        await req(1)
        await req(2)

    async def main():
        await gather(*(asyncio.create_task(request1(_)) for _ in range(4)))

    run(main())

def test_basic(session_maker, item, context_manager):


    token = None
    count = 0

    async def login():
        nonlocal token
        async with context_manager() as ctx:
            await asyncio.sleep(0)
            token = ctx.token

    async def set_data():
        nonlocal token
        nonlocal count
        async with context_manager(token):
            count += 1
            db.add(item(name='foo'))
            request.attribute  = 'bar'
            session.attribute = 'foobar'

            assert request.attribute == 'bar'
            assert session.attribute == 'foobar'

    async def get_data():
        nonlocal token
        async with context_manager(token):
            items = (await db.execute(select(item))).scalars().all()
            assert len(items) == count
            assert request.attribute == None
            assert session.attribute == 'foobar'

    async def main():
        await login()
        await set_data()
        await get_data()
        await set_data()
        await get_data()

    run(main())