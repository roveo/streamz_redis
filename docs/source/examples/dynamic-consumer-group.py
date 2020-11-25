import json
import time
from uuid import uuid4

from streamz import Stream


def preprocess(x):
    source, _id, data = x
    data["_source"] = source
    data["_id"] = _id
    return json.dumps(data)


sources = ["source-stream-1", "source-stream-2"]
name = str(uuid4())

source = Stream.from_redis_consumer_group(
    sources, "my-group", name, heartbeat_interval=10, claim_timeout=120
)
(
    source.map(preprocess)
    .partition(1000, timeout=3)
    .map(lambda x: "\n".join(x))
    .sink_to_redis_list("preprocessed-data")
)

if __name__ == "__main__":
    source.start()
    time.sleep(5)
