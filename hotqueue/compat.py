try:
    import queue
except ImportError:  # pragma: no cover
    import Queue as queue

try:
    import pickle5 as pickle
except ImportError:  # pragma: no cover
    try:
        import cPickle as pickle
    except ImportError:
        import pickle

try:
    from redislite import Redis
except ImportError:  # pragma: no cover
    from redis import Redis

__all__ = ["queue", "pickle", "Redis"]
