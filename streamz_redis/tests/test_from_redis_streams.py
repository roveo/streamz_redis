from redis import StrictRedis
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_redis.sources.from_redis_streams import from_redis_streams
from streamz_redis.tests import uuid

Stream.register_api(staticmethod)(from_redis_streams)


def test_from_redis_streams(redis: StrictRedis):
    stream = uuid()
    source = Stream.from_redis_streams({stream: 0})
    L = source.sink_to_list()
    source.start()

    data = [{"i": str(i)} for i in range(3)]
    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: len(L) == 3, 2)
    assert [x[2] for x in L] == data


def test_from_redis_streams_multiple(redis: StrictRedis):
    stream1, stream2 = uuid(2)
    source = Stream.from_redis_streams({stream1: 0, stream2: 0})
    L1 = source.pluck(0).filter(lambda x: x == stream1).sink_to_list()
    L2 = source.pluck(0).filter(lambda x: x == stream2).sink_to_list()
    source.start()

    for i in range(3):
        redis.xadd(stream1, {"i": str(i)})
        redis.xadd(stream2, {"i": str(i)})

    wait_for(lambda: len(L1) == 3, 3)
    wait_for(lambda: len(L2) == 3, 3)

    assert L1 == [stream1] * 3
    assert L2 == [stream2] * 3
