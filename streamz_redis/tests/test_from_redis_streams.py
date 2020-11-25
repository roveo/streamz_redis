from redis import StrictRedis
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_redis.sources.from_redis_streams import from_redis_streams
from streamz_redis.tests import uuid

Stream.register_api(staticmethod)(from_redis_streams)


def test_basic(redis: StrictRedis, data):
    stream = uuid()
    source = Stream.from_redis_streams(stream, timeout=0.1, default_start_id=0)
    L = source.sink_to_list()
    source.start()

    for x in data:
        redis.xadd(stream, x)

    wait_for(lambda: len(L) == 3, 2)
    assert [x[2] for x in L] == data
    source.stop()


def test_multiple(redis: StrictRedis, data):
    stream1, stream2 = uuid(2)
    source = Stream.from_redis_streams({stream1: 0, stream2: 0}, timeout=0.1)
    L1 = source.pluck(0).filter(lambda x: x == stream1).sink_to_list()
    L2 = source.pluck(0).filter(lambda x: x == stream2).sink_to_list()
    source.start()

    for x in data:
        redis.xadd(stream1, x)
        redis.xadd(stream2, x)

    wait_for(lambda: len(L1) == 3, 3)
    wait_for(lambda: len(L2) == 3, 3)

    assert L1 == [stream1] * 3
    assert L2 == [stream2] * 3
    source.stop()
