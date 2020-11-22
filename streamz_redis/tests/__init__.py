from uuid import uuid4


def uuid(n: int = None):
    if n is None:
        return str(uuid4())
    return [str(uuid4()) for _ in range(n)]
