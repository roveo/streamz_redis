import json
import time

from streamz import Stream

sources = ["data-source-1", "data-source-2"]


def preprocess(x):
    source, data = x
    data = json.reads(data)
    data["_source"] = source
    return json.dumps(data)


source = Stream.from_redis_lists(sources)
(
    source.map(preprocess)
    .partition(1000, timeout=3)
    .map(lambda x: "\n".join(x))
    .sink_to_redis_list("preprocessed-data")
)

if __name__ == "__main__":
    source.start()
    time.sleep(5)
