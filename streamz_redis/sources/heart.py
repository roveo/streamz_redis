import time
from multiprocessing import Event, Process, Queue
from typing import Union

from redis import StrictRedis
from streamz_redis.sources.consumers import convert_bytes


class Heart(Process):
    """
    A process designed to be running alongside a consumer and handling inter-consumer
    communication related to fault tolerance. Hearts will send "heartbeats" to a pub/sub
    channel named after the consumer group.
    """

    def __init__(
        self,
        streams: Union[str, list],
        group: str,
        name: str,
        client_params: dict = None,
        interval: int = 1,
        timeout=None,
        **kwargs,
    ):
        kwargs["daemon"] = True
        super().__init__(name=name, **kwargs)
        self.group = group
        self.streams = streams
        if isinstance(streams, str):
            self.streams = [streams]
        self.client_params = client_params
        self.interval = interval
        self.timeout = timeout or interval * 10
        self.heartbeats = {}
        self.last_heartbeat = None
        self.dead = Queue()
        self.stopped = Event()
        self.redis = None

    def run(self):
        self.redis = StrictRedis(**self.client_params or {})

        sub = self.redis.pubsub()
        sub.subscribe(**{self.group: self.handle_heartbeat})
        thread = sub.run_in_thread(daemon=True)

        while not self.stopped.is_set():
            self.redis.publish(self.group, self.name)
            self.check_dead()
            time.sleep(self.interval)
            if (
                self.last_heartbeat is None
                or time.time() - self.last_heartbeat > self.timeout
            ):
                break

        thread.stop()

    def stop(self):
        self.stopped.set()

    def check_dead(self):
        have_pending = set()
        for stream in self.streams:
            info = convert_bytes(self.redis.xpending(stream, self.group))
            have_pending |= set(c["name"] for c in info["consumers"])
        now = time.time()
        for con in have_pending:
            if con not in self.heartbeats:
                self.heartbeats[con] = now  # consider yet unseen consumers alive
                continue
            last = self.heartbeats[con]
            if last is None:
                continue  # skip dead
            if now - last > self.timeout:
                self.dead.put((con, last))  # notify main process that a peer is dead
                self.heartbeats[con] = None  # mark dead

    def handle_heartbeat(self, message):
        now = time.time()
        con = convert_bytes(message)["data"]
        if con == self.name:
            self.last_heartbeat = now
        else:
            self.heartbeats[con] = now
