import json
import sys
import time

from streamz import Stream


if len(sys.argv) == 2:
    _, name = sys.argv
else:
    print("Usage: python consumer.py consumer-name")
    exit()


def preprocess(x):
    source, _id, data = x
    data["_source"] = source
    data["_id"] = _id
    return json.dumps(data)


sources = ["source-stream-1", "source-stream-2"]
source = Stream.from_redis_consumer_group(sources, "my-group", name)
(
    source.map(preprocess)
    .partition(1000, timeout=3)
    .map(lambda x: "\n".join(x))
    .sink_to_redis_list("preprocessed-data")
)

if __name__ == "__main__":
    source.start()
    time.sleep(5)
