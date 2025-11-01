from multiprocessing.pool import ThreadPool

from sqlalchemy import select
from jsalchemy_web_context.sync import session, request, db



def test_proxy_dict_isolation():
    from jsalchemy_web_context.manager import ContextProxy

    prop = ContextProxy('property')
    elements = [{'foo': x} for x in range(100)]
    result = []
    tp = ThreadPool(6)

    def connect():
        prop.update(elements.pop())

    def task():
        result.append(prop['foo'])
        prop['bar'] = []
        prop['bar'].append('-')
        result.append(''.join(prop['bar']))

    def prepare(x):
        connect()
        task()

    def main():
        tp.map(prepare, [() for _ in range(10)])
        tp.map(prepare, [() for _ in range(10)])

    main()
    tp.close()
    max_dashes = max(map(len, filter(lambda x: type(x) is str and x.startswith('-'), result)))
    assert max_dashes == 1
    assert len(result) == 40

def test_context_segregation(sync_context):

    def req(token=None):
        with sync_context(token) as ctx:
            tokens.add(ctx.token)
            assert token in (None, ctx.token)
            if token:
                _ = session.foo == 'foobar', 'Session not connected'
                assert token == ctx.token, "Session doesn't reconnect"
            else:
                assert session.foo == None, 'Session not connected'
                request.foo = 'bar'
                session.foo = 'foobar'
            token = ctx.token

        with sync_context(token) as ctx:
            assert session.foo == 'foobar', 'Session not connected'
            get = request.foo
            assert get == None
            request.foo = 'bar'
            assert request.foo == 'bar', 'Request not connected'
        return token

    tokens = set()
    tp = ThreadPool(10)
    result = tp.map(req, [None for _ in range(10)])
    assert len(tokens) == 10
    tp.map(req, result * 3)
    assert len(tokens) == 10


def test_basic(sync_session_maker, sync_item, sync_context):


    token = None
    count = 0

    def login():
        nonlocal token
        with sync_context() as ctx:
            token = ctx.token

    def set_data():
        nonlocal token
        nonlocal count
        with sync_context(token):
            count += 1
            db.add(sync_item(name='foo'))
            request.attribute  = 'bar'
            session.attribute = 'foobar'

            assert request.attribute == 'bar'
            assert session.attribute == 'foobar'

    def get_data():
        nonlocal token
        with sync_context(token):
            items = db.execute(select(sync_item)).scalars().all()
            assert len(items) == count
            assert request.attribute == None
            assert session.attribute == 'foobar'

    def main():
        login()
        set_data()
        get_data()
        set_data()
        get_data()

    main()