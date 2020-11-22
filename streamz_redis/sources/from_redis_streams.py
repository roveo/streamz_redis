from typing import Union

from streamz_redis.sources.base import RedisSource
from streamz_redis.sources.consumers import Consumer
from tornado import gen


class from_redis_streams(RedisSource):
    """Consume and emit messages from one or more Redis streams."""

    def __init__(
        self,
        streams: Union[dict, str, list, tuple],
        client_params: dict = None,
        timeout: int = 0,
        count: int = None,
        default_start_id: int = "$",
        convert: bool = True,
        encoding: str = "UTF-8",
        **kwargs,
    ):
        """Parameters
        ----------
        streams: dict, str, list or tuple
            A dict of ``stream-name: message-id``. ``message-id`` is an id to start
            consuming the messages from. The first call to `.consume()` will use this
            value, subsequent calls will only return new messages. Non-existing streams
            will be created when the class is instantiated. Alternatively, can be
            ``str`` for a single stream, or ``list``/``tuple`` for multiple streams. In
            these cases message-id is equal to ``default_start_id``.
        client_params: dict
            Parameters the will be passed to ``redis-py`` client. Defaults to ``{}``.
        timeout: int or float
            Number of seconds to wait if there are no new messages in the stream. If
           there are none, this is effectively like a polling interval. Defaults to 0
           (wait indefinitely).
        count: int
            Number of items to emit at a time. If None, all available items are emitted.
            Defaults to None.
        default_start_id: str
            In cases when ``streams`` isn't a dict, this is the default starting
            message-id that's used. Defaults to `"$"`, so will start reading only new
            messages. Another possible value is `"0"` to start reading from the
            beginning of the stream.
        convert: bool
            Convert ``bytes`` in the messages to ``str``. Defaults to True.
        encoding: str
            This is the encoding that will be used to convert ``bytes`` to ``str`` if
            ``convert`` is True. Defaults to "UTF-8".
        **kwargs:
            Will be passed to ``streamz.Source``.
        """
        super().__init__(client_params=client_params, **kwargs)
        self._streams = streams
        self._timeout = timeout
        self._count = count
        self._convert = convert
        self._encoding = encoding
        self._default = default_start_id

    @gen.coroutine
    def _run(self):
        consumer = Consumer(
            client=self._redis,
            streams=self._streams,
            count=self._count,
            block=int(self._timeout * 1000),
            default_start_id=self._default,
            convert=self._convert,
            encoding=self._convert,
        )
        while not self.stopped:
            res = yield self.loop.run_in_executor(None, consumer.consume)
            if len(res) > 0:
                for stream, messages in res:
                    for _id, data in messages:
                        yield self._emit((stream, _id, data))
