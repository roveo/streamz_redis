import pytest
from redis import StrictRedis
from streamz.utils_test import wait_for
from streamz_redis.sources.consumers import GroupConsumer
from streamz_redis.sources.heart import Heart
from streamz_redis.tests import uuid


@pytest.mark.usefixtures("redis")
def test_default_timeout():
    stream, group, con = uuid(3)
    heart = Heart(stream, group, con, interval=10)

    assert heart.timeout == 100


@pytest.mark.usefixtures("redis")
def test_check_dead(redis: StrictRedis, data):
    stream, group, con1, con2 = uuid(4)
    h = Heart(stream, group, con1, timeout=0.001)

    for x in data:
        redis.xadd(stream, x)

    GroupConsumer(redis, stream, group, con2).consume()
    h.redis = redis

    h.check_dead()
    h.check_dead()

    assert h.dead.get(timeout=1)[0] == con2


def test_heartbeats(redis: StrictRedis):
    stream, group = uuid(2)

    redis.xgroup_create(stream, group, mkstream=True)

    interval = 0.1
    timeout = 0.5

    hearts = []
    for _ in range(5):
        heart = Heart(stream, group, uuid(), interval=interval, timeout=timeout)
        hearts.append(heart)
        heart.start()

    S = set()
    sub = redis.pubsub()
    sub.subscribe(group)

    def predicate():
        m = sub.get_message()
        if m is not None:
            S.add(m["data"])
        return len(S) == 5

    wait_for(predicate, 5, period=0.01)

    for h in hearts:
        h.stop()
