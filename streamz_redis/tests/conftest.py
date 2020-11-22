import pytest
import shlex
import subprocess
from streamz.utils_test import wait_for
from redis import StrictRedis


def cleanup(name="test-streamz-redis", fail=False):
    rm_cmd = shlex.split(f"docker rm -f {name}")
    try:
        subprocess.check_call(rm_cmd)
    except subprocess.CalledProcessError as e:
        print(e)
        if fail:
            raise


@pytest.fixture(scope="session")
def redis(name="test-streamz-redis"):
    cleanup(name=name)
    run_cmd = shlex.split(f"docker run -d -p 6379:6379 --name {name} redis")
    subprocess.check_call(run_cmd)

    def predicate():
        cmd = shlex.split(f"docker logs {name}")
        logs = subprocess.check_output(cmd, stderr=subprocess.STDOUT)
        return b"Ready to accept connections" in logs

    wait_for(predicate, 10, period=0.1)
    try:
        with StrictRedis() as client:
            yield client
    finally:
        cleanup(name=name, fail=True)


@pytest.fixture(scope="function")
def data(request):
    marker = request.node.get_closest_marker("n")
    if marker is None:
        n = 3
    else:
        n = marker.args[0]
    return [{"i": str(i)} for i in range(n)]
