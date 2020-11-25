from redis import StrictRedis
from streamz import Stream
from streamz_redis.sinks import sink_to_redis_list
from streamz_redis.tests import uuid


Stream.register_api()(sink_to_redis_list)


def test_sink_to_redis_list(redis: StrictRedis):
    key = uuid()
    source = Stream()
    source.sink_to_redis_list(key)

    for i in range(10):
        source.emit(i)

    assert redis.llen(key) == 10
    assert int(redis.lpop(key)) == 0
    assert int(redis.rpop(key)) == 9
