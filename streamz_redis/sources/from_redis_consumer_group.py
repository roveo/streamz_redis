from multiprocessing.queues import Empty

from streamz_redis.sources.base import RedisSource
from streamz_redis.sources.consumers import GroupConsumer
from streamz_redis.sources.heart import Heart
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
        claim_timeout: int = None,
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
        super().__init__(client_params=client_params, **kwargs)
        self._streams = streams
        self._group = group_name
        self._name = consumer_name
        self._timeout = timeout
        self._count = count
        self._replay = replay_pending
        self._client_params = client_params
        self._heartbeat_interval = heartbeat_interval
        self._claim_timeout = claim_timeout
        self._consumer = None
        self._heart = None

    def stop(self):
        if self._heart is not None:
            self._heart.stop()
        self.stopped = True

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
            self._heart = Heart(
                streams=list(self._consumer.streams),
                group=self._group,
                name=self._name,
                client_params=self._client_params,
                interval=self._heartbeat_interval,
                timeout=self._claim_timeout,
            )
            self._heart.start()
            self.loop.add_callback(self._loot)

        if self._replay:
            yield self._emit_pending()

        while not self.stopped:
            if self._heart is not None and not self._heart.is_alive():
                break
            res = yield self._run_in_executor(self._consumer.consume)
            yield self._emit_streams_response(res, ack=self._ack)

        if self._heart is not None:
            self._heart.stop()

    def _ack(self, stream, *ids):
        def cb():
            self._consumer.ack(stream, *ids)

        return cb

    @gen.coroutine
    def _emit_pending(self):
        res = yield self._run_in_executor(self._consumer.consume, True)
        yield self._emit_streams_response(res, ack=self._ack)

    @gen.coroutine
    def _loot(self):
        dead = set()
        while not self.stopped:
            dead |= self._get_dead()
            empty = set()
            for con, last in dead:
                res = self._consumer.steal_pending(con, int(last * 1000))
                messages = sum(len(m) for _, m in res)
                while messages > 0:
                    yield self._emit_streams_response(res, ack=self._ack)
                    res = self._consumer.steal_pending(con, int(last * 1000))
                    messages = sum(len(m) for _, m in res)
                empty.add((con, last))
            dead -= empty
            yield gen.sleep(self._claim_timeout)

    def _get_dead(self):
        S = set()
        while True:
            try:
                S.add(self._heart.dead.get(block=False))
            except Empty:
                break
        return S
