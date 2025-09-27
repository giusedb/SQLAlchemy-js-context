from pickle import dumps, loads

class Storage(dict):

    def __init__(self, data: dict):
        for name, value in data.items():
            setattr(self, name, value)

    def __getattr__(self, item):
        return self.get(item)

    def __setattr__(self, key, value):
        self[key] = value

    def __delattr__(self, item):
        del self[item]


    def dumps(self) -> str:
        """Serialize the session"""

        return dumps(dict(self))

    async def loads(self, pickle:str) -> 'Session':
        """Create a Session object from a piclke stiring."""
        if pickle is None:
            data = loads(pickle)
        else:
            data = {}
        return Storage(data)

class SessionManager:

    async def connect(self, token):
        """Connects current session object to the current request's token."""
        raise NotImplementedError

    async def disconnect(self, session: Storage):
        """Disconnects current session object and store the object."""
        raise NotImplementedError

    async def new(self):
        """Generate a new session object."""
        raise NotImplementedError

