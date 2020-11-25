from typing import Union

from redis import StrictRedis
from redis.exceptions import ResponseError


def convert_bytes(data, encoding="UTF-8"):
    """Recursively convert data returned from the client as bytes into strings.

    Parameters
    ----------
    data:
        Data to convert. Can be bytes, list, dict or tuples nested in any way.
    encoding: str
        Encoding to use when decoding bytes. Defaults to ``UTF-8``.
    """
    if isinstance(data, bytes):
        return data.decode(encoding)
    if isinstance(data, list):
        return list(map(convert_bytes, data))
    if isinstance(data, dict):
        return dict(map(convert_bytes, data.items()))
    if isinstance(data, tuple):
        return tuple(map(convert_bytes, data))
    return data


class Consumer:
    """Helper class to consume messages from a number of streams. Basically a stateful
    wrapper around Redis ``XREAD`` command. Keeps track of received messages during its
    run, but it's up to the user to save state between runs.

    Parameters
    ----------
    client: redis.StrictRedis
        ``redis-py`` client instance to use.
    streams: dict
        A dict of ``stream-name: message-id``. ``message-id`` is an id to start
        consuming the messages from. The first call to `.consume()` will use this value,
        subsequent calls will only return new messages.
    count: int
        Number of messages to read from the stream at one time. If ``None``, all
        messages are received. Defaults to ``None``.
    block: int
        Number of milliseconds to block for before returning no messages (empty list
        for each stream), if no new messages are added to the stream. Defaults to 0
        (will block until messages are received).
    default_start_id: str
        In cases when ``streams`` isn't a dict, this is the default starting message-id
        that's used. Defaults to `"$"`, so will start reading only new messages. Another
        possible value is `"0"` to start reading from the beginning of the stream.
    convert: bool
        Convert ``bytes`` in the messages to ``str``. Defaults to True.
    encoding: str
        This is the encoding that will be used to convert ``bytes`` to ``str`` if
        ``convert`` is True. Defaults to "UTF-8".
    """

    def __init__(
        self,
        client: StrictRedis,
        streams: Union[str, dict, tuple, list],
        count: int = None,
        block: int = 0,
        default_start_id: str = "$",
        convert: bool = True,
        encoding: str = "UTF-8",
    ):
        self.streams = self._convert_streams(streams, default_start_id)
        self.client = client
        if block is None:
            raise ValueError(
                "block must be an int, non-blocking XREAD is not supported"
            )
        self.block = block
        self.count = count
        self.convert = convert
        self.encoding = encoding

    @staticmethod
    def _convert_streams(s, default):
        if isinstance(s, str):
            return {s: default}
        if isinstance(s, (list, tuple)):
            return {x: default for x in s}
        if isinstance(s, dict):
            return s
        raise ValueError("streams must be dict, str, list or tuple")

    def _preprocess(self, data):
        if self.convert:
            return convert_bytes(data, encoding=self.encoding)
        return data

    def consume(self, count: int = None, block: int = None):
        """Consume messages from streams.

        Converts the ``bytes`` in the message to ``str``.

        Parameters
        ----------
        count: int
            Optionally override default ``count``. Defaults to ``None``.
        block: int
            Optionally override default ``block``. Defaults to ``None``.
        """
        _count = count or self.count
        _block = block or self.block
        res = self._preprocess(
            self.client.xread(self.streams, block=_block, count=_count)
        )
        for stream, messages in res:
            self.streams[stream] = max(i for (i, _) in messages)
        return res


class GroupConsumer(Consumer):
    """Helper class to consume messages as a consumer group. A wrapper around Redis
    ``XREADGROUP`` command.

    Parameters
    ----------
    client: StrictRedis
        ``redis-py`` client instance to use.
    streams: dict, str, list or tuple
        A dict of ``stream-name: message-id``. ``message-id`` is an id to start
        consuming the messages from. The first call to `.consume()` will use this value,
        subsequent calls will only return new messages. Non-existing streams will be
        created when the class is instantiated.

        Alternatively, can be ``str`` for a single stream, or ``list``/``tuple`` for
        multiple streams. In these cases message-id is presumed to be ``0``.
    group_name: str
        Name of Redis consumer group. The group will be created for each stream
        in ``streams``.
    consumer_name: str
        Name of Redis consumer in the consumer group.
    count: int
        Number of messages to read from the stream at one time. If ``None``, all
        messages are received. Defaults to ``None``.
    block: int
        Number of milliseconds to block for before returning no messages (empty list
        for each stream), if no new messages are added to the stream. ``None`` is not
        allowed. Defaults to 0 (wait indefinitely).
    convert: bool
        Convert ``bytes`` in the messages to ``str``. Defaults to True.
    encoding: str
        This is the encoding that will be used to convert ``bytes`` to ``str`` if
        ``convert`` is True. Defaults to "UTF-8".
    """

    def __init__(
        self,
        client: StrictRedis,
        streams: Union[str, dict, tuple, list],
        group_name: str,
        consumer_name: str,
        count: int = None,
        block: int = 0,
        convert: bool = True,
        encoding: str = "UTF-8",
    ):
        super().__init__(
            client=client,
            streams=streams,
            count=count,
            block=block,
            convert=convert,
            encoding=encoding,
        )
        self.group = group_name
        self.name = consumer_name
        self.ensure_group()

    def ensure_group(self):
        """Ensure the consumer group exists for each of the streams, create the streams
        if necessary. This operation is idempotent.
        """
        for stream, _ in self.streams.items():
            try:
                self.client.xgroup_create(stream, self.group, id="0", mkstream=True)
            except ResponseError:
                pass

    def read(self, pending=False):
        """Read messages from streams.

        Parameters
        ----------
        pending: bool
            Read messages from PEL (pending entry list) instead of new messages.
            Useful for initial recovery.
        """
        _id = "0" if pending else ">"
        _count = None if pending else self.count
        streams = {s: _id for s in self.streams}
        return self._preprocess(
            self.client.xreadgroup(
                self.group,
                self.name,
                streams,
                count=_count,
                block=self.block,
            )
        )

    def consume(self, pending=False):
        """Consume messages from streams.

        Parameters
        ----------
        pending: bool
            Read messages from PEL (pending entry list) instead of new messages.
            Useful for initial recovery. Defaults to False.
        """
        return self.read(pending)

    def ack(self, stream, *ids):
        """Acknowledge a number of message ids."""
        self.client.xack(stream, self.group, *ids)

    def get_pending(self, stream, consumer, count):
        """Get a list of pending messages belonging to a consumer."""
        messages = self._preprocess(
            self.client.xpending_range(
                name=stream,
                groupname=self.group,
                min="-",
                max="+",
                count=count,
                consumername=consumer,
            )
        )
        return [x["message_id"] for x in messages]

    def claim_pending(self, stream, consumer, min_idle_time, count=None):
        """Claim pending messages in a stream from another consumer. Messages are
        returned as a usual batch (with stream name) for consistency. Claiming messages
        is useful when a consumer in the group is down for a long time (possibly
        forever) and we need to recover unacknoledged messages.
        """
        _count = count or self.count or 1000  # have to provide a count

        ids = self.get_pending(stream, consumer, _count)
        if len(ids) > 0:
            claimed = self._preprocess(
                self.client.xclaim(
                    name=stream,
                    groupname=self.group,
                    consumername=self.name,
                    min_idle_time=0,
                    message_ids=ids,
                )
            )
            return [[stream, claimed]]  # add stream name to mimic the usual batch
        return [[stream, []]]

    def steal_pending(self, consumer, min_idle_time):
        """Steal pending messages in all streams from another consumer in this
        group.
        """
        res = []
        for stream in self.streams:
            res.extend(self.claim_pending(stream, consumer, min_idle_time))
        return res
