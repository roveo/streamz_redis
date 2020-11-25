Redis Lists Tutorial
====================

A list is one of the basic `Redis data types`_. It's a collection of elements that you
can add items to and retrieve items from. Items can be "popped" (read and simultaneously
removed) or just read without removing them. A list has two sides: the head (or left
side) and the tail (right). Adding/popping can be done on either end, so a list can
be used as a FIFO/FILO queue.

Relevant commands:

- `RPUSH`_ and `LPUSH`_
- `LPOP`_ and `RPOP`_

.. _Redis data types: https://redis.io/topics/data-types
.. _RPUSH: https://redis.io/commands/rpush
.. _LPUSH: https://redis.io/commands/lpush
.. _LPOP: https://redis.io/commands/lpop
.. _RPOP: https://redis.io/commands/rpop

Stream nodes
------------

There are two stream nodes for working with lists:
:class:`streamz_redis.sources.from_redis_lists` for reading items and
:class:`streamz_redis.sinks.sink_to_redis_list` for writing them.

Both nodes take an optional ``client_params`` argument that defaults to ``{}``. It will
be passed directly to ``redis-py`` ``Connection`` class, so if you leave it empty, it
assumes that Redis is at ``localhost:6379`` and the database ID is ``0``.

Reading data from Redis lists
-----------------------------

Lists are very simple to work with, but there are serious tradeoffs. They are not made
with resilience and fault-tolerance in mind. One you pop items from a list, they are
gone. If your consumer crashes while there are data in memory, it's lost forever with no
chance of retrieval. Nonetheless, lists are a popular way to process streams of data.

Let's say somebody is writing JSON data to a couple of lists (one per data source). You
want to preprocess the data and enrich it:

- add data source name to JSON
- partition the data into batches of JSON records, one per line
- add the data to another list for further processing

.. literalinclude:: examples/lists-consumer.py
    :language: python


Items are emitted from the source as tuples: ``(list-name, item)``. The source allows us
to read data from multiple lists and we might be interested in where it came from
further down the pipeline. Source lists can be specified as an iterable or, in case of a
single list, just a ``str``.

We receive items from the list and partition them into batches of 1000 elements, but
don't allow a batch to live longer than 3 seconds, even it's incomplete. The purpose of
this is not to bug Redis too often, but also deliver fresh data on time.

Then, we add a newline between elements and append an additional newline at the end,
resulting in a number of JSON records separated by newlines. After that, the data is
written to a list named ``preprocessed-data``.

Then we start our source and have it run forever.

If you want to scale at some point, just run several copies of this script. The data
will go to the consumers in a round-robin fashion, so the consumers will share the
workload, but never get the same data.

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

.. _BLPOP: https://redis.io/commands/blpop
.. _BRPOP: https://redis.io/commands/brpop
