from concurrent.futures import ThreadPoolExecutor

import pytest
from redis import StrictRedis
from streamz_redis.sources.consumers import Consumer, GroupConsumer, convert_bytes
from streamz_redis.tests import uuid


def just_data(res):
    for _, messages in res:
        return [x for _, x in messages]


def test_convert():
    data = [[b"stream", [(b"0", {b"i": 1}), (b"1", {b"i": b"x"})]]]
    assert convert_bytes(data) == [["stream", [("0", {"i": 1}), ("1", {"i": "x"})]]]


def test_consumer_defaults(redis: StrictRedis, data):
    stream = uuid()
    consumer = Consumer(redis, stream)

    redis.xadd(stream, {"i": -1})  # this will not be consumed

    with ThreadPoolExecutor() as pool:
        future = pool.submit(consumer.consume)
        for x in data:
            redis.xadd(stream, x)
        res = future.result()

    assert len(res) > 0
    assert just_data(res) == data[:1]

    res = consumer.consume()
    assert len(res) > 0
    assert just_data(res) == data[1:]


def test_consumer_default_id_zero(redis: StrictRedis, data):
    stream = uuid()
    consumer = Consumer(redis, stream, default_start_id="0")

    redis.xadd(stream, data[0])  # this will be consumed
    res = consumer.consume()
    assert just_data(res) == [data[0]]

    redis.xadd(stream, data[1])
    res = consumer.consume()
    assert just_data(res) == [data[1]]


def test_consumer_multiple_streams(redis: StrictRedis, data):
    stream1, stream2 = uuid(2)
    consumer = Consumer(redis, {stream1: 0, stream2: 0})

    for x in data:
        redis.xadd(stream1, x)
        redis.xadd(stream2, x)

    res = consumer.consume()
    part1, part2 = res

    assert [x for (_, x) in part1[1]] == data
    assert [x for (_, x) in part2[1]] == data


def test_group_consumer_ensure_group(redis: StrictRedis):
    s1, s2, group, con = uuid(4)
    consumer = GroupConsumer(redis, {s1: 0, s2: 0}, group, con)

    consumer.ensure_group()
    consumer.ensure_group()  # idempotent

    assert redis.xinfo_groups(s1) != []
    assert redis.xinfo_groups(s2) != []


@pytest.mark.n(10)
def test_group_consumer_consume(redis: StrictRedis, data):
    s1, s2, group, con = uuid(4)
    consumer = GroupConsumer(redis, {s1: 0, s2: 0}, group, con)

    for x in data:
        if int(x["i"]) % 2 == 0:
            redis.xadd(s1, x)
        else:
            redis.xadd(s2, x)

    res = consumer.consume()
    assert len(res) > 0
    for _, messages in res:
        assert len(messages) == len(data) // 2

    for x in data:
        redis.xadd(s1, x)

    res = consumer.consume()
    assert len(res) > 0
    for _, messages in res:
        assert len(messages) == len(data)


@pytest.mark.n(6)
def test_group_consumer_pending(redis: StrictRedis, data):
    stream, group, con = uuid(3)
    consumer = GroupConsumer(redis, {stream: 0}, group, con)

    for x in data:
        redis.xadd(stream, x)

    assert len(consumer.get_pending(stream, con, 10)) == 0

    res = consumer.consume()
    assert len(res) > 0

    for stream, messages in res:
        consumer.ack(stream, *[_id for _id, _ in messages][:3])

    assert len(consumer.get_pending(stream, con, 10)) == 3

    res = consumer.consume(pending=True)

    assert len(res) > 0
    for stream, messages in res:
        assert [x for _, x in messages] == data[3:]


@pytest.mark.n(10)
def test_group_consumer_claim(redis: StrictRedis, data):
    stream, group, con = uuid(3)
    consumer = GroupConsumer(redis, {stream: 0}, group, con)

    for x in data:
        redis.xadd(stream, x)

    consumer.consume()

    consumer.name = uuid()  # pretent do be a different consumer

    for _, messages in consumer.claim_pending(stream, con, count=3):
        assert len(messages) == 3

    consumer.count = 2  # test claiming all in batches
    for _, messages in consumer.claim_pending(stream, con, count=None):
        assert len(messages) == 7
