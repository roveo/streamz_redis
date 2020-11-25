Quickstart
==========

Setup
-----

First, install:

.. code-block:: sh

    pip install git+https://github.com/roveo/streamz_redis.git


Then, start a throwaway Redis in Docker:

.. code-block:: sh

    docker run --rm -p 6379:6379 --name test-streamz-redis redis


Sources
-------

``streamz_redis`` can read data from Redis lists or streams. There are different sources
for different modes of operation.


``from_redis_list``
*******************

The simplest possible source is ``from_redis_list``. It will pop items from the head
of the list and emit them:

.. code-block:: python

    from streamz import Stream
    from redis import Redis
    from time import sleep


    source = Stream.from_redis_list("test-list")
    source.sink(print)
    source.start()

    with Redis() as client:
        for i in range(5):
            client.rpush(i)
    sleep(1)

Note that ``RPUSH`` is used here for adding new items: we're adding to the tail of
the list and reading from the head, like in a FIFO queue. ``LPUSH`` will get you FILO
behavior.

If your pipeline crashes, the messages held in memory will be lost. For a more
fault-tolerant solution, see :ref:`from_redis_consumer_group <from-redis-consumer-group>`
below.


``from_redis_streams``
**********************

Make sure that you familiarized yourself with `Redis Streams`_ before continuing.

In its simplest form, ``from_redis_streams`` reads messages from a single stream,
starting from the first message received since ``.start()`` was called, and continues
emitting as new messages arrive.

.. code-block:: python

    source = Stream.from_redis_streams("test-stream")
    source.sink(print)

    with Redis() as client:
        client.xadd("test-stream", {"i": -1})  # this will not be emitted
        source.start()
        for i in range(5):
            client.xadd("test-stream", {"i": i})
    sleep(1)


``from_redis_consumer_group``
*****************************

Allows you to create a consumer group with several consumers. The consumers share the
workload and Redis keeps track of which messages are delivered to each consumer,
allowing for recovery in case of consumer failure.

.. code-block:: python

    out = Source().rate_limit(0.01)
    out.sink(print)

    for i in range(3):
        s = Source.from_redis_consumer_group(
            "test-stream", "test-group", f"consumer-{i}"
        )
        s.connect(out)
        s.start()

    with Redis() as client:
        for i in range(10):
            client.xadd("test-stream", {"i": i})
    sleep(1)

The three consumers in this example will run in parallel, receiving messages that weren't
delivered to other consumers. In addition to this, when messages leave the pipeline
through the sink, the source will report to Redis that they were successfully processed
(a process known as message acknoledgement). When a consumer crashes, it loses messages
stored in its memory that are delivered, but not processesed. When it's restarted with
the same name, by default it will retrieve any unacknowledged messages, so no messages
are lost.

What's next
-----------

For a more thorough overview of sources, please see :doc:`tutorial`.

.. _Redis Streams: https://redis.io/topics/streams-intro
