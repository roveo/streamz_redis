from redis import StrictRedis
from streamz import Stream
from streamz.utils_test import wait_for
from streamz_redis.sources.from_redis_lists import from_redis_lists
from streamz_redis.tests import uuid

Stream.register_api(staticmethod)(from_redis_lists)


def test_from_redis_lists(redis: StrictRedis):
    name = uuid()
    source = Stream.from_redis_lists(name)
    L = source.pluck(1).map(int).sink_to_list()
    source.start()

    redis.rpush(name, *list(range(3)))

    wait_for(lambda: L == [0, 1, 2], 3)


def test_from_redis_lists_multiple(redis: StrictRedis):
    l1, l2 = uuid(2)

    source = Stream.from_redis_lists([l1, l2])
    L = source.pluck(1).map(int).sink_to_list()
    source.start()

    redis.rpush(l1, *list(range(3)))
    redis.rpush(l2, *list(range(3)))

    wait_for(lambda: len(L) == 6, 2)
