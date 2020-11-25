from redis import StrictRedis
from streamz import Stream


class RedisNode(Stream):
    """Base class for Redis stream nodes.

    Parameters
    ----------

    client_params: dict
        Will be passed to ``redis-py`` client instance. Defaults to None.
    """

    def __init__(self, *args, client_params: dict = None, **kwargs):
        self._params = client_params or {}
        self._client = None
        super().__init__(*args, **kwargs)

    @property
    def _redis(self) -> StrictRedis:
        """``redis-py`` client instance bound to this source. Will be created
        when first accessed.
        """
        if self._client is None:
            self._client = StrictRedis(**self._params)
        return self._client
