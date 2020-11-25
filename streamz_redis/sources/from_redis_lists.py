from typing import Union

from streamz_redis.sources.base import RedisSource
from tornado import gen


class from_redis_lists(RedisSource):
    """Emit items from a number of Redis lists. Uses ``BLPOP``, so the lists are
    used as a volatile queue, FIFO or FILO depending on the combination of how the
    elements are added to the list and ``left`` parameter.

    Note that if there is a crash, there's no way to retrieve unprocessed items that
    were popped from the list. If you need durability, consider using
    ``from_redis_consumer_group``.
    """

    def __init__(
        self,
        keys: Union[list, str],
        client_params: dict = None,
        timeout: int = 0,
        left: bool = True,
        **kwargs
    ):
        """
        Parameters
        ----------
        keys: str or list-like
            One or more Redis lists to read from.
        client_params: dict
            Parameters the will be passed to ``redis-py`` client. Defaults to ``{}``.
        timeout: int or float
            Number of seconds to wait if the list is empty. If ``0``, will block until
            new items are added to the list. Defaults to ``0``.
        left: bool
            Use ``BLPOP`` if ``True``, ``BRPOP`` otherwise. Defaults to ``True``.
        **kwargs:
            Will be passed to ``streamz.Source``.
        """
        super().__init__(client_params=client_params, **kwargs)
        if isinstance(keys, str):
            self._keys = [keys]
        else:
            self._keys = keys
        self._timeout = timeout
        self._left = left
        self._popmethod = None

    def _pop(self):
        return self._popmethod(self._keys, timeout=self._timeout)

    @gen.coroutine
    def _run(self):
        self._popmethod = self._redis.blpop if self._left else self._redis.brpop
        while not self.stopped:
            x = yield self._run_in_executor(self._pop)
            if x is not None:
                yield self._emit(x)
