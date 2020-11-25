from streamz import Stream
import time


def test_run_and_fail():
    L = []

    def run_and_fail():
        source = Stream()
        sink = source.buffer(10).rate_limit(1).sink(L.append)

        for i in range(10):
            source.emit(i)

        sink.destroy()

    run_and_fail()

    for _ in range(10):
        time.sleep(0.1)
        assert len(L) == 1
