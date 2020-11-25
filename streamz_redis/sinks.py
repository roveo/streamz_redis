from typing import Union

from streamz import Sink
from streamz_redis.base import RedisNode


class sink_to_redis_list(RedisNode, Sink):
    def __init__(self, upstream, key: Union[list, str], right=True, **kwargs):
        self._key = key
        self._right = right
        super().__init__(upstream, **kwargs)

    def update(self, x, who=None, metadata=None):
        if self._right:
            self._redis.rpush(self._key, x)
        else:
            self._redis.lpush(self._key, x)
