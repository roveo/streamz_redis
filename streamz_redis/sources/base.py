from streamz import Source
from streamz.core import RefCounter
from streamz_redis.base import RedisNode
from tornado import gen


def create_metadata(cb):
    return [{"ref": RefCounter(cb=cb)}]


class RedisSource(Source, RedisNode):
    """Abstract class for redis sources.

    Parameters
    ----------

    client_params: dict
        Will be passed to ``redis-py`` client instance. Defaults to None.
    """

    def __init__(self, **kwargs):
        super().__init__(ensure_io_loop=True, **kwargs)

    def start(self):
        self.stopped = False
        self.loop.add_callback(self._run)

    def _run_in_executor(self, fn, *args):
        """Shorthand for running something in a thread."""
        return self.loop.run_in_executor(None, fn, *args)

    @gen.coroutine
    def _emit_streams_response(self, result, ack=None):
        """Emits individual messages from a batch received from the client.

        Client response looks like this:
        [
            [stream-name-1, [
                (message-id, message-data),
                (message-id, message-data),
            ]],
            [stream-name-2, [
                (message-id, message-data),
                (message-id, message-data),
                (message-id, message-data),
            ]]
        ]

        The events will be emitted as a 3-tuple:
        (stream-name, message-id, message-data)

        The events are emitted individually rather than in batches, as they are received
        from the client. This is because we don't want them to share metadata.
        If the batch gets split later on in the pipeline, messages in the batch will be
        acknowledged only when all of them are processed, which can lead to reading them
        twice in case of pipeline crash and recovery.
        """
        for stream, messages in result:
            for _id, data in messages:
                if callable(ack):
                    m = create_metadata(ack(stream, _id))
                else:
                    m = None
                yield self._emit((stream, _id, data), metadata=m)
