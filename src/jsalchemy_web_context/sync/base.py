from pickle import dumps, loads
from ..base import Storage

class SessionManager:

    def connect(self, token):
        """Connects current session object to the current request's token."""
        raise NotImplementedError

    def disconnect(self, session: Storage):
        """Disconnects current session object and store the object."""
        raise NotImplementedError

    def new(self):
        """Generate a new session object."""
        raise NotImplementedError

