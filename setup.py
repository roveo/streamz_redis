from setuptools import setup, find_packages


setup(
    name="streamz_redis",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "streamz @ git+https://github.com/python-streamz/streamz.git",
        "redis>=3.5.3",
    ],
    entry_points={
        "streamz.sources": [
            "from_redis_list = streamz_redis.sources:from_redis_list",
            "from_redis_stream = streamz_redis.sources:from_redis_stream",
            "from_redis_comsumer_group = "
            "streamz_redis.sources:from_redis_consumer_group",
        ],
        "streamz.nodes": [],
        "streamz.sinks": [],
    },
)
