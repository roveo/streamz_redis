import pytest
from redis import StrictRedis
from streamz import Stream
from streamz_redis.sinks import sink_to_redis_list, sink_to_redis_stream
from streamz_redis.tests import uuid

Stream.register_api()(sink_to_redis_list)
Stream.register_api()(sink_to_redis_stream)


def test_list(redis: StrictRedis):
    key = uuid()
    source = Stream()
    source.sink_to_redis_list(key)

    for i in range(10):
        source.emit(i)

    assert redis.llen(key) == 10
    assert int(redis.lpop(key)) == 0
    assert int(redis.rpop(key)) == 9


def test_list_left(redis: StrictRedis):
    key = uuid()
    source = Stream()
    source.sink_to_redis_list(key, right=False)

    for i in range(10):
        source.emit(i)

    assert redis.llen(key) == 10
    assert int(redis.lpop(key)) == 9
    assert int(redis.rpop(key)) == 0


def test_stream(redis: StrictRedis, data):
    key = uuid()
    source = Stream()
    source.sink_to_redis_stream(key)

    for x in data:
        source.emit(x)

    assert redis.xlen(key) == 3


@pytest.mark.n(50)
def test_stream_maxlen(redis: StrictRedis, data):
    key = uuid()
    source = Stream()
    source.sink_to_redis_stream(key, maxlen=10, approximate=False)

    for x in data:
        source.emit(x)

    assert redis.xlen(key) == 10
