from collections import defaultdict
from itertools import groupby
from operator import attrgetter
from typing import List, Dict, Any, Callable

from sqlalchemy import event, Insert, Update, select, Select, Delete
from sqlalchemy.orm import DeclarativeBase, Session, RelationshipDirection
from sqlalchemy.orm.unitofwork import UOWTransaction

class ResultData:
    def __init__(self):
        self.new = set()
        self.update = set()
        self.delete = {}
        self.extra = {}
        self.m2m = []
        self.invalids = {}

    def update_diff(self, request):
        """Generate a dict with the data difference of the updated records."""
        on_load = {model: {x['id']: x for x in items} for model, items in request.loaded.items() }
        ret = {}
        for model, grp in groupby(sorted(self.update, key=lambda x: type(x).__name__), type):
            load_model = on_load[model]
            if not load_model:
                continue
            ret[model] = {}
            col_attrs = model.__mapper__.columns.keys()
            for item in grp:
                load_item = load_model[item.id]
                serial = ((attr, getattr(item, attr)) for attr in col_attrs)
                diff = {attr: value for attr, value in serial if value != load_item[attr]}
                if diff:
                    ret[model][item.id] = diff
            if not ret[model]:
                del ret[model]
        return ret

    def clear(self):
        self.new.clear()
        self.update.clear()
        self.delete.clear()
        self.extra.clear()
        self.m2m.clear()
        self.invalids.clear()

    def __repr__(self):
        summary = {k: v for k, v in ((k, len(getattr(self, k))) for k in self.__slots__) if v}
        return f"ResultData: {summary}"

    __slots__ = ('extra', 'delete', 'm2m', 'new', 'update', 'invalids')


class ChangeInterceptor:
    """
    A class responsible for intercepting and tracking changes made to SQLAlchemy models
    during a session, including new, updated, and deleted entities.

    It integrates with the JSAlchemy API's resource manager to serialize objects
    and capture change information for use in tracking or reporting.
    """

    def __init__(self, after_commit: Callable = None, request = None):
        """
        Initialize the ChangeInterceptor with a ResourceManager instance.

        Args:
            resource_manager (ResourceManager): The manager used to serialize
                                               model instances into data representations.
        """
        self.call_back = after_commit
        self._session_trackers: Dict[Session, dict] = {}
        self.register_session(Session)
        self.table_to_model = {}
        self._pks = {}
        self.request = request
        if not request:
            from .manager import request as req
            self.request = req

    def start_record(self):
        """
        Initialize tracking of changes by setting up a ResultData object
        in the current request context.
        """
        self.request.result = ResultData()
        self.request.loaded = defaultdict(list)

    def end_transaction(self):
        """
        End the transaction and call the callback function if provided.
        """
        if self.call_back:
            self.call_back(self.request.result)

    def register_session(self, session: Session):
        """
        Register a SQLAlchemy session for change tracking.

        Listens to flush and commit events on the given session to capture
        entity changes.

        Args:
            session (Session): The SQLAlchemy ORM session to monitor.
        """
        # event.listen(session, 'before_commit', self._on_after_commit)
        event.listen(session, 'before_flush', self._on_before_flush)
        event.listen(session, 'after_soft_rollback', self._on_rollback)
        event.listen(session, 'do_orm_execute', self._on_orm_execute)

    def _on_orm_execute(self, execute_state):
        stmt = execute_state.statement
        if isinstance(stmt, Select):
            return
        tracker: ResultData = self.request.result
        model = self.table_to_model.get(stmt.table)
        if not model:
            return
        pk = next(iter(stmt.table.primary_key.columns))
        if isinstance(execute_state.statement, Insert):
            if stmt._multi_values:
                self.request.result.new.update({model(**val) for chunk in  stmt._multi_values for val in chunk})
            else:
                self.request.result.new.update({model(**stmt._values)})
        elif isinstance(stmt, Update):
            if model not in tracker.invalids:
                tracker.invalids[model] = set()
            updated = execute_state.session.execute(select(pk).where(stmt.whereclause)).scalars().all()
            tracker.invalids[model].update(updated)
        elif isinstance(stmt, Delete):
            if model not in tracker.delete:
                tracker.delete[model] = set()
            deleted = execute_state.session.execute(select(pk).where(stmt.whereclause)).scalars().all()
            tracker.delete[model].update(deleted)

    def _on_rollback(self, session: Session, previous):
        self.start_record()

    def _on_before_flush(self, session: Session, transaction: UOWTransaction, pk=None, *args):
        """
        Callback triggered before a flush occurs. Tracks new, updated, and deleted
        entities in the session.

        Args:
            session (Session): The session being flushed.
            transaction (UOWTransaction): The unit of work transaction.
            *args: Additional arguments passed by the event system.
        """
        tracker = self.request.result
        if session.dirty:
            tracker.update.update(session.dirty.difference(tracker.new))
            tracker.new.difference_update(tracker.update)
        if session._new:
            tracker.new.update(set(session.new))
        if session._deleted:
            deleted = { model: set(map(attrgetter(self._pks[model]), grp))
                        for model, grp in groupby(sorted(session.deleted, key=lambda x: type(x).__name__), type)
                        if model in self._pks }

            new = { model: set(map(attrgetter(self._pks[model]), grp))
                    for model, grp in groupby(sorted(tracker.new, key=lambda x: type(x).__name__), type)
                    if model in self._pks }
            for model, ids in deleted.items():
                (tracker.delete
                    .setdefault(model, set())
                    .update(
                        ids.difference(new.get(model, set()))))
                getter = attrgetter(self._pks[model])
                tracker.new.difference_update(
                    filter(lambda x: type(x) == model and getter(x) in ids,
                           tracker.new))
                tracker.update.difference_update(
                    filter(lambda x: type(x) == model and getter(x) in ids,
                           tracker.update))


    def _on_before_commit(self, session):
        """
        Callback triggered before a commit. Stores tracked changes in the request context.

        Args:
            session (Session): The session about to be committed.
        """
        print('--> before commit <--')
        self.request.results = {
            'new': self.new,
            'updated': self.updated,
            'deleted': self.deleted,
            'm2m': self.m2m,
        }

    def _load_model(self, target: DeclarativeBase, context):
        """
        Callback triggered when a model is loaded from the database.
        Stores a serialized copy of the loaded object for later diffing.

        Args:
            target (DeclarativeBase): The model instance that was loaded.
            context: The load event context.
        """
        self.request.loaded[target.__class__].append(
            {attr: getattr(target, attr) for attr in (target.__mapper__.columns.keys())})

    def _m2m_append(self, target, value, initiator):
        """
        Callback triggered when an item is added to a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being added.
            initiator: The event initiator (used to get the property name).
        """
        self.request.result.m2m.append(('add', type(value).__name__, initiator.key, [value.id, target.id]))

    def _m2m_remove(self, target, value, initiator):
        """
        Callback triggered when an item is removed from a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being removed.
            initiator: The event initiator (used to get the property name).
        """
        self.request.result.m2m.append(('del', type(value).__name__, initiator.key, [value.id, target.id]))

    def register_model(self, model):
        """
        Register all many-to-many relationships of a model to listen for changes.
        Also registers the load event listener to capture object footprints.
        Args:
            model: The SQLAlchemy declarative base model class to register.
        """
        self.table_to_model[model.__table__] = model
        self._pks[model] = next(iter(model.__mapper__.primary_key)).key
        event.listen(model, 'load', self._load_model)
        m2ms = (prop for prop in model.__mapper__.relationships if
                prop.direction == RelationshipDirection.MANYTOMANY)

        for m2m in m2ms:
            event.listen(m2m, 'append', self._m2m_append)
            event.listen(m2m, 'remove', self._m2m_remove)
