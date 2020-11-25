from time import sleep

import pytest
from redis import StrictRedis
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_redis.sinks import sink_to_redis_list
from streamz_redis.sources import from_redis_consumer_group
from streamz_redis.sources.consumers import convert_bytes
from streamz_redis.tests import uuid
from tornado.queues import Queue

Stream.register_api(staticmethod)(from_redis_consumer_group)
Stream.register_api()(sink_to_redis_list)


def test_ack(redis: StrictRedis, data):
    stream, group, con = uuid(3)
    source = Stream.from_redis_consumer_group(stream, group, con, timeout=0.1)
    L = source.sink_to_list()

    for x in data:
        redis.xadd(stream, x)

    source.start()

    wait_for(lambda: len(L) == 3, 3, lambda: print(L))
    sleep(0.05)  # wait a bit for the last ack
    for _, messages in redis.xreadgroup(group, con, {stream: 0}):
        assert messages == []

    source.stop()


@pytest.mark.n(10)
def test_replay(redis: StrictRedis, data):
    stream, group, con = uuid(3)

    maxsize = 5
    source = Stream.from_redis_consumer_group(
        {stream: 0}, group, con, count=1, timeout=0.1
    )
    buffer = source.buffer(maxsize)
    L = buffer.rate_limit(0.1).sink_to_list()
    source.start()

    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: buffer.queue.qsize() == maxsize, 1)
    buffer.queue = Queue(maxsize)
    source.stop()

    wait_for(lambda: len(L) == 3, 1)

    source.start()

    wait_for(lambda: len(L) == 10, 2)
    assert set(x[2]["i"] for x in L) == set(x["i"] for x in data)

    source.stop()


@pytest.mark.n(50)
def test_multiple_consumers(redis: StrictRedis, data):
    out = Stream().pluck(2)
    S = set()
    out.pluck("i").sink(S.add)

    stream, group = uuid(2)
    sources = set()
    for _ in range(3):
        con = uuid()
        source = Stream.from_redis_consumer_group(
            stream,
            group,
            con,
            count=1,
            timeout=0.1,
        )
        source.connect(out)
        source.start()
        sources.add(source)

    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: len(S) == 50, 1)
    assert S == set(x["i"] for x in data)

    for s in sources:
        s.stop()


@pytest.mark.n(500)
def test_claim(redis: StrictRedis, data):
    stream, group, target = uuid(3)

    for x in data:
        redis.xadd(stream, x)

    def run_and_fail():
        name = uuid()
        source = Stream.from_redis_consumer_group(
            stream,
            group,
            name,
            count=1,
            timeout=0.1,
        )
        buffer = source.buffer(10)
        buffer.rate_limit(0.1).pluck(1).sink_to_redis_list(target)
        source.start()

        wait_for(lambda: buffer.queue.qsize() == 10, 3)
        buffer.queue = Queue(10)  # lose data in the buffer, won't be ACKed
        source.stop()

        def pending_10():
            cons = convert_bytes(redis.xpending(stream, group))["consumers"]
            for con in cons:
                if con["name"] == name and con["pending"] == 10:
                    return True
            return False

        wait_for(pending_10, 1, period=0.1)

    for _ in range(10):
        run_and_fail()

    source = Stream.from_redis_consumer_group(
        stream,
        group,
        uuid(),
        heartbeat_interval=0.1,
        claim_timeout=1,
        count=10,
        timeout=0.1,
    )
    source.pluck(1).sink_to_redis_list(target)
    source.start()

    wait_for(
        lambda: redis.llen(target) == 500,
        15,
        lambda: print(redis.llen(target)),
        period=0.1,
    )

    source.stop()
