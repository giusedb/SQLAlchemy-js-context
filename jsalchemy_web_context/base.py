from pickle import dumps, loads

class Storage:

    def __init__(self, data: dict):
        for name, value in data.items():
            setattr(self, name, value)

    def dumps(self) -> str:
        """Serialize the session"""
        return dumps(self.__dict__)

    async def loads(self, pickle:str) -> 'Session':
        """Create a Session object from a piclke stiring."""
        if data:
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

