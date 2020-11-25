Redis Streams Tutorial
======================

A stream is an append-only data structure that is a collection of messages. A message,
in Python terms, is a ``dict``, although Redis allows the keys to be duplicate, so it's
closer to a list of ``(key, value)`` tuples. But in practice, most messages will be a
``dict``.

Before continuing, please make sure that you made yourself familiar with `Introduction
to Redis Streams`_ and the relevant Redis commands:

- XADD_
- XREAD_
- XGROUP_
- XREADGROUP_

.. _Introduction to Redis Streams: https://redis.io/topics/streams-intro
.. _XADD: https://redis.io/commands/xadd
.. _XREAD: https://redis.io/commands/xread
.. _XGROUP: https://redis.io/commands/xgroup
.. _XREADGROUP: https://redis.io/commands/xgroupread


Stream nodes
------------

There are several stream nodes related to streams:

.. autosummary::
    streamz_redis.sources.from_redis_streams
    streamz_redis.sources.from_redis_consumer_group

``from_redis_streams`` is available, but it offers little on top of just using lists:
you probably won't lose messages, but crash recovery and keeping track of what's already
been processed is on the user. Because ``from_redis_consumer_group`` adds just a little
more complexity (just providing a consumer group name), but offers greater benefits,
we'll concentrate on it.

Simple example
--------------

Let's replicate the lists example, but with streams. Let's also parametrize our script,
so that you can provide a name for our consumer from the command line:

.. literalinclude:: examples/simple-consumer-group.py
    :language: python

Then you can run it like this: ``python consumer.py stream-consumer-1``.

This is very similar to what we already saw with lists, but with some improvements. If
the consumer crashes, you won't lose data: on restart, if consumer name is the same, it
will retrieve whichever messages were delivered, but not succesfully processed. Redis
keeps track of which messages were delivered to each consumer in a group, and when
messages exit the pipeline, they get "acknowledged", telling Redis that the consumer is
done with them.

All stream sources emit messages as a 3-tuple: ``(stream-id, message-id, data)``.
``preprocess`` function adds stream-id and message-id to the data and returns it as
JSON. Then we batch the data and write to a list.

If you have several exit points in your pipeline, for example you're writing to a
database and to files at the same time, it might be possible that at the moment of a
crash, a message is written to one place, but not the other. In this the message won't
get acknowledged and on restart, you consumer will reclaim the message once again,
resulting in duplicates.

Just like with lists, consumers in the same group will not get the same messages. But
you can have many consumer groups listening to the same list. For example, one consumer
group can write raw data to a database table, and another one write aggregated summary
metrics to some other table.

If you want to scale, just run another consumer with a different name. Just make sure
that consumers are restarted when crashed and the names don't change.

When the client receives messages, all strings are represented as ``bytes`` objects. The
source converts them to ``str``, by default using ``UTF-8`` encoding. If you have python
sending the messages, there's probably nothing to worry about. But just in case, you can
control this by either changing the encoding with ``encoding`` option or turning it off
altogether with ``convert=False``.

The ``streams`` argument is required and allows you to specify the streams to read from.
Again, like with lists, it can be either a list of names, or a ``str`` for a single
stream. You can also provide it as a dict (like ``redis-py`` expects), but in practice
it's not necessary, because the source will figure out the correct starting message IDs
for you.

Redis client receives messages in batches, not one by one. This can lead to problems,
because if there are a lot of messages to be recevied, you can run out of memory
quickly. This is controlled by ``count`` argument, which defaults to ``None``. It means
that the source will receive all the messages there are to receive.

The ``timeout`` option works the same way as with lists. The default is ``0`` for
blocking indefinitely. Internally the client uses milliseconds, so you can work with
fractions, although Redis's current time resolution is about 0.1 seconds, so specifying
less then that will not affect anything. Less than 0.001 is the same as 0.

Dynamic consumer names
----------------------

In some scenarios (e.g. a Kubernetes replica set) it's not easy to control consumer
names, so you might want to generate them dynamically. You might also want to scale you
group up and down, so some consumers will exit and not come back online.

For these use cases, there are two additional options available: ``heartbeat_interval``
and ``claim_timeout``.

.. literalinclude:: examples/dynamic-consumer-group.py

Consumers will send heartbeats to the group's pub/sub channel at some interval
(specified by ``heartbeat_interval`` option). If a consumer that has unacknowledged
messages fails to send a heartbeat in ``claim_timeout`` seconds, other consumers will
claim its unacknowledged messages, preventing data loss.

A consumer's heart runs as a separate process. Even if the source in the main event loop
is blocked by running CPU-intensive computations, heartbeats will be sent on time. If
the heart fails for some reason, the source will recognize it and stop itself. The heart
also receives keeps track of it's own heartbeats, so if there are delays or problems
with connection, it will also stop, in turn stopping the source.
