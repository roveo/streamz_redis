from time import sleep

import pytest
from redis import StrictRedis
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_redis.sources.from_redis_consumer_group import from_redis_consumer_group
from streamz_redis.tests import uuid
from tornado.queues import Queue

Stream.register_api(staticmethod)(from_redis_consumer_group)


def test_from_redis_consumer_group_ack(redis: StrictRedis, data):
    stream, group, con = uuid(3)
    source = Stream.from_redis_consumer_group(stream, group, con)
    L = source.sink_to_list()

    for x in data:
        redis.xadd(stream, x)

    source.start()

    wait_for(lambda: len(L) == 3, 3)
    sleep(0.05)  # wait a bit for the last ack
    for _, messages in redis.xreadgroup(group, con, {stream: 0}):
        assert messages == []


@pytest.mark.n(10)
def test_from_redis_consumer_group_replay(redis: StrictRedis, data):
    stream, group, con = uuid(3)

    maxsize = 5
    source = Stream.from_redis_consumer_group(
        {stream: 0}, group, con, count=1, timeout=0.001
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


@pytest.mark.n(50)
def test_from_redis_consumer_group_multiple_consumers(redis: StrictRedis, data):
    out = Stream().pluck(2)
    S = set()
    out.pluck("i").sink(S.add)

    stream, group = uuid(2)
    for _ in range(3):
        con = uuid()
        source = Stream.from_redis_consumer_group({stream: 0}, group, con)
        source.connect(out)
        source.start()

    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: len(S) == 50, 1)
    assert S == set(x["i"] for x in data)


@pytest.mark.usefixtures("redis")
def test_from_redis_consumer_group_heartbeat():
    stream, group = uuid(2)
    source1 = Stream.from_redis_consumer_group(
        {stream: 0}, group, uuid(), heartbeat_interval=0.1
    )
    source2 = Stream.from_redis_consumer_group(
        {stream: 0}, group, uuid(), heartbeat_interval=0.1
    )
    source1.start()
    source2.start()

    wait_for(lambda: len(source1._heartbeats) == 1, 0.2)
    wait_for(lambda: len(source2._heartbeats) == 1, 0.2)


@pytest.mark.n(50)
def test_from_redis_consumer_group_claim(redis: StrictRedis, data):
    """In some environments (e.g. replica sets in k8s) it might be useful
    to generate a unique name for each new consumer and have the group
    figure out how to continue normal operation after a crash.
    """
    out = Stream().rate_limit(0.1)
    L = out.sink_to_list()

    stream, group = uuid(2)
    maxsize = 5

    def create_source(stream, group, con):
        source = Stream.from_redis_consumer_group(
            {stream: 0},
            group,
            con,
            heartbeat_interval=0.1,
            claim_timeout=1,
            count=1,
            timeout=0.001,
        )
        buffer = source.buffer(maxsize)
        return source, buffer

    source, _ = create_source(stream, group, uuid())
    source.connect(out)
    source.start()

    failing_id = uuid()
    failing, buffer = create_source(stream, group, failing_id)
    buffer.connect(out)
    failing.start()

    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: buffer.queue.qsize() == maxsize, 1)
    buffer.queue = Queue(maxsize)
    failing.stop()

    wait_for(lambda: len(source._heartbeats) == 1, 1)
    wait_for(lambda: len(L) == 50, 5)
    assert set(x[2]["i"] for x in L) == set(str(x) for x in range(50))
