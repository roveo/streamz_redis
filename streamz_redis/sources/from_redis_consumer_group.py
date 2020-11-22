import time

from streamz_redis.sources.base import RedisSource
from streamz_redis.sources.consumers import GroupConsumer, convert_bytes
from tornado import gen


class from_redis_consumer_group(RedisSource):
    """Consume messages from one or more Redis streams as a member of a consumer
    group.
    """

    def __init__(
        self,
        streams: dict,
        group_name: str,
        consumer_name: str,
        client_params: dict = None,
        timeout: int = 0,
        count: int = None,
        replay_pending: bool = True,
        heartbeat_interval: int = None,
        claim_timeout: int = 10,
        **kwargs,
    ):
        """Parameters
        ----------
        streams: dict, str, list or tuple
            A dict of ``stream-name: message-id``. ``message-id`` is an id to start
            consuming the messages from. The first call to ``.consume()`` will use this
            value, subsequent calls will only return new messages. Non-existing streams
            will be created when the class is instantiated.

            Alternatively, can be ``str`` for a single stream, or ``list``/``tuple`` for
            multiple streams. In these cases message-id is presumed to be ``0``.
        group_name: str
            Name of Redis consumer group. The group will be created for each stream
            in ``streams``.
        consumer_name: str
            Name of Redis consumer in the consumer group.
        client_params: dict
            Parameters the will be passed to ``redis-py`` client. Defaults to ``{}``.
        timeout: int or float
            Number of seconds to wait if there are no new messages in the stream.
            Defaults to 0 (wait indefinitely).
        count: int
            Number of items to emit at a time. If None, all available items are emitted.
            Defaults to None.
        replay_pending: bool
            Retrieve messages from PEL (pending entry list) when started. Defaults to
            True.
        heartbeat_interval: int
            Interval at which this source will send heartbeats to the group's pub/sub
            channel. Defaults to None (heartbeats are turned off).
        claim_timeout: int
            Number of seconds after which a consumer is considered dead and other
            consumers are free to steal its unacknowledged messages. The source will not
            steal messages if it's not sending heartbeats (the default). Defaults to 10.
        convert: bool
            Convert ``bytes`` in the messages to ``str``. Defaults to True.
        encoding: str
            This is the encoding that will be used to convert ``bytes`` to ``str`` if
            ``convert`` is True. Defaults to "UTF-8".
        **kwargs:
            Will be passed to ``streamz.Source``.
        """
        super().__init__(client_params, **kwargs)
        self._streams = streams
        self._group = group_name
        self._name = consumer_name
        self._timeout = timeout
        self._count = count
        self._replay = replay_pending
        self._heartbeat_interval = heartbeat_interval
        self._claim_timeout = claim_timeout
        self._heartbeats = {}
        self._consumer = None

    @gen.coroutine
    def _run(self):
        self._consumer = GroupConsumer(
            client=self._redis,
            streams=self._streams,
            group_name=self._group,
            consumer_name=self._name,
            count=self._count,
            block=int(self._timeout * 1000),
        )

        if self._heartbeat_interval is not None:
            self.loop.add_callback(self._heartbeat)

        if self._replay:
            yield self._emit_pending()
        while not self.stopped:
            res = yield self._run_in_executor(self._consumer.consume)
            yield self._emit_streams_response(res, ack=self._ack)

    def _ack(self, stream, *ids):
        def cb():
            self._consumer.ack(stream, *ids)

        return cb

    @gen.coroutine
    def _emit_pending(self):
        res = yield self._run_in_executor(self._consumer.consume, True)
        yield self._emit_streams_response(res, ack=self._ack)

    @gen.coroutine
    def _heartbeat(self):
        sub = self._redis.pubsub()
        sub.subscribe(**{self._group: self._handle_heartbeat})
        thread = sub.run_in_thread()
        while not self.stopped:
            self._redis.publish(self._group, self._name)
            yield self._cleanup()
            yield gen.sleep(self._heartbeat_interval)
        thread.stop()

    def _handle_heartbeat(self, message):
        con = convert_bytes(message)["data"]
        if con != self._name:
            self._heartbeats[con] = time.time()

    @gen.coroutine
    def _cleanup(self):
        for con, last in self._heartbeats.items():
            now = time.time()
            if now - last > self._claim_timeout:
                res = yield self._run_in_executor(self._consumer.steal_pending, con)
                yield self._emit_streams_response(res)
