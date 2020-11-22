Tutorial
========

This is a practical tutorial on how to use ``streamz_redis``. If you need a quick guide,
see :doc:`quickstart`.

Reading data from Redis lists
-----------------------------

For very simple use cases, if you don't care about data loss or consumer crash
probability is low, you can use Redis lists as a queue. Let's say somebody is pushing
JSON data to a Redis list at ``my-list`` key and you want to write the data to a file.

.. code-block:: python

    from streamz import Stream
    import weakref
    from time import sleep


    class TextFileWriter:

        def __init__(self, filename):
            self.fp = open(filename, "a")
            weakref.finalize(self, self.fp.close)  # make sure the file is closed

        def __call__(self, x):
            self.fp.write(x)


    client_params = dict(host="redis.example.com", db=1)
    source = Stream.from_redis_lists("my-list", client_params=client_params)
    (
        source.partition(1000, timeout=3)
        .map(lambda x: "\n".join(x) + "\n")
        .sink(TextFileWriter("data.json"))
    )

    if __name__ == "__main__":
        source.start()
        time.sleep(5)

We receive items from the list and partition them into batches of 1000 elements, but
don't allow a batch to live longer than 3 seconds, even it's incomplete. The purpose of
this is not to bug the filesystem too often, but also deliver fresh data on time.

Then, we add a newline between elements and append an additional newline at the end,
resulting in a number of JSON records separated by newlines. After that, the data is
written to ``data.json``.

Then we start out source and have it run forever.

``client_params`` is a common argument for all Redis sources. It defaults to ``{}``
(empty dict) and will be passed to `redis-py connection class`_. If your Redis server is
at ``localhost:6379`` and there are not fancy settings involved, you can just ignore it.

If you want to listen to several lists, you can specify a list of names, like this:

.. code-block:: python

    Stream.from_redis_lists(["list-1", "list-2"])

By default, the source will read data from the "left" side (or "head") of the list. You
can set ``left=False`` to read from the right (or "tail").

.. code-block:: python

    Source.from_redis_lists("my-list", left=False)  # read from the tail

Depending on which side the items are added to, you get different behavior. If you read
from where the items are added, you get last-in-first-out (LIFO) queue. If the sides are
different, you get first-in-first-out (FIFO).

The default (``left=True``) will use `BLPOP`_ command behind the scenes. ``left=False``
will use `BRPOP`_.

In most cases the ``timeout`` option should be left as is: if a list is empty, the
client will just block until it isn't. The client waits for items in a thread, so if you
know that new items will be added rarely and you'd like to avoid having very
long-running threads, just set this to a reasonable number of seconds.

Plain Redis Streams
-------------------

A stream is an append-only data structure that is a collection of messages. A message,
in Python terms, is a ``dict``, although Redis allows the keys to be duplicate, so it's
closer to a list of ``(key, value)`` tuples. But in practice, most messages will be a
``dict``.

The simplest source here is ``from_redis_streams``. Let's replicate our lists example,
but with streams. I'll skip the already imported packages, the "run forever" loop and
the ``TextFileWriter`` class.

.. code-block:: python

    import json

    def convert_message(message):
        stream, _id, data = message.copy()
        data["_redis_stream"] = stream
        data["_redis_message_id"] = _id
        return data

    source = Stream.from_redis_streams(streams="my-stream")
    (
        source.map(convert_message)
        .map(json.dumps)
        .partition(1000, timeout=3)
        .map(lambda x: "\n".join(x) + "\n")
        .sink(TextFileWriter("data.json"))
    )

All stream sources emit messages as a 3-tuple: ``(stream-id, message-id, data)``.
``convert_message`` function adds stream-id and message-id to the data and returns the
resulting dict. Then we convert this to JSON and proceed as usual.

When the client receives messages, all strings are represented as ``bytes`` objects. The
source converts them to ``str``, by default using ``UTF-8`` encoding. If you have python
sending the messages, there's probably nothing to worry about. But just in case, you can
control this by either changing the encoding with ``encoding`` option or turning it off
altogether with ``convert=False``.

The ``streams`` argument is required and allows you to specify the streams to read from.
This source uses `XREAD`_ to read the messages, so it requres the starting message ID.
The most explicit way to specify streams is to pass a dict of stream names and message
IDs:

.. code-block:: python

    Source.from_redis_streams({"stream-1": "$", "stream-2": "$"})

If you don't want to specify message IDs, you can just use the short notation: either
just a single stream name (like in the example) as a ``str`` or a list of names. In this
case, the starting message ID is controlled by the ``default_start_id`` argument. By
default it's ``"$"``, so the sources will only will receive messages that arrived since
it started listening. You can specify ``"0"`` to read from the start of the stream or
any other valid message ID.

These sources are equivalent:

.. code-block:: python

    # same thing
    Source.from_redis_streams({"my-stream": "0"})
    Source.from_redis_streams("my-stream", default_start_id="0")

Redis client receives messages in batches, not one by one. This can lead to problems,
because if there are a lot of messages to be recevied, you can run out of memory
quickly. This is controlled by ``count`` argument, which defaults to ``None``. It means
that the source will receive all the messages there are to receive.

The ``timeout`` option works the same way as with lists. The default is ``0`` for
blocking indefinitely. Internally the client uses milliseconds, so you can work with
fractions, although Redis's current time resolution is about 0.1 seconds, so specifying
less then that will not affect anything. Less than 0.001 is the same as 0.

Consumer Groups
---------------

In most production settings, you want to use consumer groups. In some sense, all the
previous examples in this tutorial exist just to make you familiar with common source
settings.

Consumer groups allow you to achieve several things at once:

1. Share the workload between processors, so you can have several consumers processing
the data.

2. Be fault-tolerant. Consumers can pick up where they left off after a
restart.

3. Even if a consumer is dead forever (for example, you'd like to generate unique
consumer names for a k8s replica set), other consumers can recognize this and clean up
after it.

Let's see how a simple consumer app might look in a production setting:

.. code-block:: python

    from time import time
    from uuid import uuid4
    from streamz import Stream

    name = str(uuid4())

    source = Stream.from_redis_consumer_group(
        streams=["stream-1", "stream-2"],
        group_name="streaming",
        consumer_name=name,
        client_params=...,
        heartbeat_interval=5,
        claim_timeout=50,
    )

    if __name__ == "__main__":
        source.start()
        while True:
            sleep(5)

The new parameters here are ``group_name`` and ``consumer_name``. Consumers in the same
group will not receive the same messages and thus will share the workload between them.
Consumers are identified by their name. In this case, names are globally unique, so if a
consumer is dead, it's dead forever.

Consumers will send heartbeats to the group's pub/sub channel every 5 seconds (specified
by ``heartbeat_interval`` option). If a consumer that previously sent heartbeats fails
to do so in 50 seconds (``claim_timeout``), other consumers will claim its
unacknowledged messages, preventing data loss.

If consumer names don't change and a consumer is restarted, it will try to reclaim its
unacknowledged messages. This is the default behavior controlled by ``replay_pending``
boolean argument.

Because of how consumer groups work, you never have to specify streams in full notation
(with a dict). You can, but it doesn't make sense to do it. Consumers will always get
unacknowledged messages first, and then continue to receive new messages as they arrive
to the stream.

If you have several exit points in your pipeline, for example you're writing to a
database and to files at the same time, it might be possible that at the moment of a
crash, a message is written to one place, but not the other. In this case Redis isn't
notified know that this message was processed succesfully and on restart, you consumer
will reclaim the message once again, resulting in duplicates.


.. _BLPOP: https://redis.io/commands/blpop
.. _BRPOP: https://redis.io/commands/brpop
.. _XREAD: https://redis.io/commands/xread
.. _redis-py connection class:
    https://redis-py.readthedocs.io/en/stable/#redis.Connection
