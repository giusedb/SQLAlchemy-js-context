from collections import defaultdict
from copy import deepcopy
from itertools import groupby
from operator import itemgetter
from typing import List, Dict, Any, Callable

from sqlalchemy import event
from sqlalchemy.orm import DeclarativeBase, Session, RelationshipDirection
from sqlalchemy.orm.unitofwork import UOWTransaction

from .manager import request


class ResultData:
    def __init__(self):
        self.new = set()
        self.update = set()
        self.delete = set()
        self.description = []
        self.m2m = []

    def update_diff(self):
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
        self.description.clear()
        self.m2m.clear()

    def __repr__(self):
        summary = {k: v for k, v in ((k, len(getattr(self, k))) for k in self.__slots__) if v}
        return f"ResultData: {summary}"

    __slots__ = ('description', 'delete', 'm2m', 'new', 'update')


class ChangeInterceptor:
    """
    A class responsible for intercepting and tracking changes made to SQLAlchemy models
    during a session, including new, updated, and deleted entities.

    It integrates with the JSAlchemy API's resource manager to serialize objects
    and capture change information for use in tracking or reporting.
    """

    def __init__(self, after_commit: Callable = None):
        """
        Initialize the ChangeInterceptor with a ResourceManager instance.

        Args:
            resource_manager (ResourceManager): The manager used to serialize
                                               model instances into data representations.
        """
        self.call_back = after_commit
        self._session_trackers: Dict[Session, dict] = {}
        self.register_session(Session)

    def start_record(self):
        """
        Initialize tracking of changes by setting up a ResultData object
        in the current request context.
        """
        request.result = ResultData()
        request.loaded = defaultdict(list)

    def end_transaction(self):
        """
        End the transaction and call the callback function if provided.
        """
        if self.call_back:
            self.call_back(request.result)

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

    def _on_delete(self, mapper, connection, target: DeclarativeBase):
        request.result.delete.add(target)

    def _on_rollback(self, session: Session, previous):
        self.start_record()

    @property
    def changes(self) -> Dict[str, Any]:
        """
        Retrieve the current set of tracked changes from the request context.

        Returns:
            Dict[str, Any]: The full change tracking dictionary.
        """
        return request.tracker

    @property
    def m2m(self) -> List[Any]:
        """
        Placeholder for many-to-many relationship changes.
        Returns:
            List[Any]: Placeholder list (currently empty).
        """
        return []

    def _on_before_flush(self, session: Session, transaction: UOWTransaction, *args):
        """
        Callback triggered before a flush occurs. Tracks new, updated, and deleted
        entities in the session.

        Args:
            session (Session): The session being flushed.
            transaction (UOWTransaction): The unit of work transaction.
            *args: Additional arguments passed by the event system.
        """
        tracker = request.result
        if session.dirty:
            tracker.update.update(session.dirty.difference(tracker.new))
            tracker.new.difference_update(tracker.update)
        if session._new:
            tracker.new.update(set(session.new))
        if session._deleted:
            tracker.delete.update(session.deleted.difference(tracker.new))
            tracker.new.difference_update(session.deleted)
            tracker.update.difference_update(session.deleted)


    def _on_before_commit(self, session):
        """
        Callback triggered before a commit. Stores tracked changes in the request context.

        Args:
            session (Session): The session about to be committed.
        """
        print('--> before commit <--')
        request.results = {
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
        request.loaded[target.__class__].append(
            {attr: getattr(target, attr) for attr in (target.__mapper__.columns.keys())})

    def _m2m_append(self, target, value, initiator):
        """
        Callback triggered when an item is added to a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being added.
            initiator: The event initiator (used to get the property name).
        """
        request.result.m2m.append(('add', type(value).__name__, initiator.key, [value.id, target.id]))

    def _m2m_remove(self, target, value, initiator):
        """
        Callback triggered when an item is removed from a many-to-many relationship.

        Args:
            target: The source object of the M2M relation.
            value: The related object being removed.
            initiator: The event initiator (used to get the property name).
        """
        request.result.m2m.append(('del', type(value).__name__, initiator.key, [value.id, target.id]))

    def register_model(self, model):
        """
        Register all many-to-many relationships of a model to listen for changes.
        Also registers the load event listener to capture object footprints.
        Args:
            model: The SQLAlchemy declarative base model class to register.
        """
        event.listen(model, 'load', self._load_model)
        m2ms = (prop for prop in model.__mapper__.relationships if
                prop.direction == RelationshipDirection.MANYTOMANY)

        for m2m in m2ms:
            event.listen(m2m, 'append', self._m2m_append)
            event.listen(m2m, 'remove', self._m2m_remove)
