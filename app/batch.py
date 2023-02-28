from pipe import Pipe


@Pipe
def batch(iterable, size):
    b = []
    for i in iterable:
        b.append(i)
        if len(b) == size:
            yield b
            b = []
    if b:
        yield b
