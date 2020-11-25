from typing import Union

from streamz import Sink
from streamz_redis.base import RedisNode


class sink_to_redis_list(RedisNode, Sink):
    """Push items to a Redis list."""

    def __init__(self, upstream, key: str, right=True, **kwargs):
        """
        Parameters
        ----------
        key: str
            List name.
        right: bool
            Defaults to ``True``. Push items to the tail end of the list (use ``RPUSH``
            command). Otherwise, push to the head (use ``LPUSH``).
        """
        super().__init__(upstream, **kwargs)
        self._key = key
        self._right = right

    def update(self, x, who=None, metadata=None):
        if self._right:
            self._redis.rpush(self._key, x)
        else:
            self._redis.lpush(self._key, x)


class sink_to_redis_stream(RedisNode, Sink):
    """Write messages to a Redis stream."""

    def __init__(self, upstream, key: str, maxlen=None, approximate=True, **kwargs):
        """
        Parameters
        ----------
        key: str
            Stream name.
        maxlen: int
            Defaults to ``None``. Don't allow the stream to be longer than this size.
        """
        super().__init__(upstream, **kwargs)
        self._key = key
        self._maxlen = maxlen
        self._approximate = approximate

    def update(self, x, who=None, metadata=None):
        self._redis.xadd(
            self._key, x, maxlen=self._maxlen, approximate=self._approximate
        )
