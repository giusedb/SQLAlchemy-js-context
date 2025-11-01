import pytest
import pytest_asyncio
from sqlalchemy import insert, update, delete

from jsalchemy_web_context import db, request
from jsalchemy_web_context.interceptors import ChangeInterceptor, ResultData


@pytest_asyncio.fixture
async def intercepted(context, models):
    Container, Item, OtherItem = models


    intercepted = ResultData()

    def on_intercepts(data):
        intercepted.clear()
        intercepted.update.update(data.update)
        intercepted.delete.update(data.delete)
        intercepted.new.update(data.new)
        intercepted.extra = data.extra
        intercepted.m2m.extend(data.m2m)
        intercepted.invalids = data.invalids


    context.change_interceptor = ChangeInterceptor(on_intercepts, request=request)
    context.change_interceptor.register_model(Container)
    context.change_interceptor.register_model(Item)
    context.change_interceptor.register_model(OtherItem)

    async with context():
        db.add(Container(name='first container', id=1))
        db.add(Item(name='first item', id=1, container_id=1))
        db.add(OtherItem(name='first other item', id=1, container_id=1))

    return (intercepted,) + models

@pytest.mark.asyncio
async def test_interceptor_basic(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = Container(name="test")
        db.add(container)

    assert len(intercepted.new) == 1
    assert len(intercepted.update) == 0
    assert len(intercepted.delete) == 0
    assert container in intercepted.new

@pytest.mark.asyncio
async def test_interceptor_basic_2(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = (await db.get(Container, 1))
        container.name = 'test2'
        item = Item(name='test', container=container)
        db.add(item)

    assert len(intercepted.new) == 1
    assert len(intercepted.update) == 1
    assert len(intercepted.delete) == 0
    assert container in intercepted.update
    assert item in intercepted.new

@pytest.mark.asyncio
async def test_interceptor_basic_3(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = (await db.get(Container, 1))
        item = (await db.get(Item, 1))
        other = OtherItem(name='test', container=container)
        db.add(other)
        db.delete(item)
        await db.rollback()

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 0
    assert len(intercepted.delete) == 0

@pytest.mark.asyncio
async def test_interceptor_basic_4(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = (await db.get(Container, 1))
        item = (await db.get(Item, 1))
        other = OtherItem(name='test', container=container)
        db.add(other)
        await db.delete(item)

    assert len(intercepted.new) == 1
    assert len(intercepted.update) == 1
    assert len(intercepted.delete) == 1
    assert container in intercepted.update
    assert item.id in intercepted.delete[type(item)]
    assert other in intercepted.new


@pytest.mark.asyncio
async def test_interceptor_multi_commit_1(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = (await db.get(Container, 1))
        other = (await db.get(OtherItem, 1))
        container.name = 'test2'
        item = Item(name='test', container=container)
        db.add(item)
        await db.delete(other)

        await db.rollback()

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 0
    assert len(intercepted.delete) == 0
    assert container not in intercepted.new

@pytest.mark.asyncio
async def test_interceptor_multi_commit_2(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = (await db.get(Container, 1))
        other = (await db.get(OtherItem, 1))
        container.name = 'test2'
        item = Item(name='test', container=container)
        db.add(item)
        await db.delete(other)

        await db.commit()
        item = Item(name='after commit', container=container)
        db.add(item)

    assert len(intercepted.new) == 2
    assert len(intercepted.update) == 1
    assert len(intercepted.delete) == 1
    assert container in intercepted.update
    assert item in intercepted.new
    assert other.id in intercepted.delete[type(other)]

@pytest.mark.asyncio
async def test_interceptor_multi_commit_3(context, intercepted):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = Container(name="More changes")
        db.add(container)
        await db.commit()
        container.name = 'test2'
        item = Item(name='test', container=container)
        db.add(item)
        await db.commit()

    assert len(intercepted.new) == 2
    assert len(intercepted.update) == 0
    assert len(intercepted.delete) == 0
    assert container in intercepted.new
    assert item in intercepted.new


@pytest.mark.asyncio
async def test_interceptor_update_attributes(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        item = await db.get(Item, 1)
        other = await db.get(OtherItem, 1)
        item.name = 'test2'
        other.name = 'test2'

    assert intercepted.update_diff(request)[Item] == {1: {'name': 'test2'}}
    assert intercepted.update_diff(request)[OtherItem] == {1: {'name': 'test2'}}
    assert len(intercepted.update) == 2
    assert len(intercepted.delete) == 0
    assert len(intercepted.new) == 0
    assert item in intercepted.update
    assert other in intercepted.update


@pytest.mark.asyncio
async def test_interceptor_m2m_1(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        item = await db.get(Item, 1)
        other = await db.get(OtherItem, 1)
        other.name = 'same name'
        (await item.awaitable_attrs.other_items).append(other)

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 2
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 2
    assert intercepted.update_diff(request)[OtherItem] == {1: {'name': 'same name'}}
    assert other in intercepted.update
    assert ('add', 'OtherItem', 'other_items', [1, 1]) in intercepted.m2m
    assert ('add', 'Item', 'other_items', [1, 1]) in intercepted.m2m


@pytest.mark.asyncio
async def test_interceptor_m2m_2(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = Container(name="tes2")
        item = Item(name="tes2", container=container)
        other = OtherItem(name="tes2", container=container)
        db.add_all([container, item, other])

    async with context():
        item = await db.get(Item, 1)
        other = await db.get(OtherItem, 2)
        (await item.awaitable_attrs.other_items).append(other)

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 2
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 2
    assert item in intercepted.update
    assert other in intercepted.update

    assert ('add', 'OtherItem', 'other_items', [2, 1]) in intercepted.m2m
    assert ('add', 'Item', 'other_items', [1, 2]) in intercepted.m2m

@pytest.mark.asyncio
async def test_interceptor_m2m_delete(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = Container(name="tes2")
        item = Item(name="tes2", container=container)
        old_item = await db.get(Item, 1)
        old_other = await db.get(OtherItem, 1)
        other = OtherItem(name="tes2", container=container)
        (await item.awaitable_attrs.other_items).append(other)
        (await item.awaitable_attrs.other_items).append(old_other)
        (await old_item.awaitable_attrs.other_items).append(other)
        (await old_item.awaitable_attrs.other_items).append(old_other)
        db.add_all([container, item, other])

    async with context():
        item_1 = await db.get(Item, 1)
        item_2 = await db.get(Item, 2)
        other_1 = await db.get(OtherItem, 1)
        other_2 = await db.get(OtherItem, 2)
        (await item_1.awaitable_attrs.other_items).remove(other_1)
        (await item_1.awaitable_attrs.other_items).remove(other_2)

    assert len(intercepted.new) == 0
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 4

    assert ('del', 'Item', 'other_items', [1, 2]) in intercepted.m2m
    assert ('del', 'Item', 'other_items', [1, 1]) in intercepted.m2m
    assert ('del', 'OtherItem', 'other_items', [2, 1]) in intercepted.m2m
    assert ('del', 'OtherItem', 'other_items', [1, 1]) in intercepted.m2m


@pytest.mark.asyncio
async def test_interceptor_m2m_delete_multicommit(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        container = Container(name="tes2")
        item = Item(name="tes2", container=container)
        old_item = await db.get(Item, 1)
        old_other = await db.get(OtherItem, 1)
        other = OtherItem(name="tes2", container=container)
        (await item.awaitable_attrs.other_items).append(other)
        (await item.awaitable_attrs.other_items).append(old_other)
        (await old_item.awaitable_attrs.other_items).append(other)
        (await old_item.awaitable_attrs.other_items).append(old_other)
        db.add_all([container, item, other])

    async with context():
        item_1 = await db.get(Item, 1)
        item_2 = await db.get(Item, 2)
        other_1 = await db.get(OtherItem, 1)
        other_2 = await db.get(OtherItem, 2)
        (await item_1.awaitable_attrs.other_items).remove(other_1)
        await db.commit()
        (await item_1.awaitable_attrs.other_items).remove(other_2)

    assert len(intercepted.new) == 0
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 4

    assert ('del', 'Item', 'other_items', [1, 2]) in intercepted.m2m
    assert ('del', 'Item', 'other_items', [1, 1]) in intercepted.m2m
    assert ('del', 'OtherItem', 'other_items', [2, 1]) in intercepted.m2m
    assert ('del', 'OtherItem', 'other_items', [1, 1]) in intercepted.m2m

@pytest.mark.asyncio
async def test_interceptor_bulk_insert(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        query = insert(Container.__table__).values(
            [{'name': f'Container {n}', 'id': n} for n in range(1000, 2000)])
        raw_sql = str(query.compile(compile_kwargs={'literal_binds': True}))
        await db.execute(query)
        await db.commit()

    assert len(intercepted.new) == 1000
    assert {type(x) for x in intercepted.new} == {Container}
    assert {x.id for x in intercepted.new} == {x for x in range(1000, 2000)}
    assert len(intercepted.update) == 0
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 0

@pytest.mark.asyncio
async def test_interceptor_bulk_update(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        query = insert(Container.__table__).values(
            [{'name': f'Container {n}', 'id': n} for n in range(1000, 2000)])
        raw_sql = str(query.compile(compile_kwargs={'literal_binds': True}))
        await db.execute(query)
        await db.commit()

    async with context():
        query = update(Container.__table__).where(Container.__table__.columns.id == 1001).values(name='suca')
        await db.execute(query)

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 0
    assert len(intercepted.invalids) == 1
    assert intercepted.invalids[Container] == {1001}
    assert len(intercepted.delete) == 0
    assert len(intercepted.m2m) == 0

@pytest.mark.asyncio
async def test_interceptor_bulk_delete(intercepted, context):
    intercepted, Container, Item, OtherItem = intercepted

    async with context():
        query = insert(Container.__table__).values(
            [{'name': f'Container {n}', 'id': n} for n in range(1000, 2000)])
        raw_sql = str(query.compile(compile_kwargs={'literal_binds': True}))
        await db.execute(query)
        await db.commit()

    async with context():
        query = delete(Container).where(Container.name.startswith('Container'))
        await db.execute(query)

    assert len(intercepted.new) == 0
    assert len(intercepted.update) == 0
    assert len(intercepted.invalids) == 0
    assert len(intercepted.delete[Container]) == 1000
    assert len(intercepted.m2m) == 0
    assert intercepted.delete[Container] == set(range(1000, 2000))


