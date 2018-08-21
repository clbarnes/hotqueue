# -*- coding: utf-8 -*-

"""HotQueue is a Python library that allows you to use Redis as a message queue
within your Python programs.
"""

from __future__ import unicode_literals
from functools import wraps
try:
    from queue import Empty
except ImportError:
    from Queue import Empty
from abc import ABCMeta, abstractmethod
from enum import Enum

from six import add_metaclass, string_types

try:
    import cPickle as pickle
except ImportError:
    import pickle

from redis import Redis


__all__ = ['LifoHotQueue', 'FifoHotQueue', 'HotQueue', 'DeHotQueue']

__version__ = '0.2.8'
__version_info__ = tuple(int(v) for v in __version__.split('.'))


class End(Enum):
    FRONT = 'l'
    BACK = 'r'

    @classmethod
    def from_str(cls, val):
        if isinstance(val, cls):
            return val
        elif isinstance(val, string_types):
            lower = val.lower()
            if lower.startswith('l') or lower.startswith('f'):
                return cls.FRONT
            elif lower.startswith('r') or lower.startswith('b'):
                return cls.BACK
            else:
                raise ValueError("Unrecognised string '{}'".format(val))
        else:
            raise TypeError(
                "Expects Side or string type, got {}".format(type(val).__name__)
            )

    def opposite(self):
        if self == End.FRONT:
            return End.BACK
        else:
            return End.FRONT


def key_for_name(name):
    """Return the key name used to store the given queue name in Redis."""
    return 'hotqueue:{}'.format(name)


@add_metaclass(ABCMeta)
class BaseHotQueue(object):
    """Simple message queue stored in a Redis list. Example:

    >>> from hotqueue import HotQueue
    >>> queue = HotQueue("myqueue", host="localhost", port=6379, db=0)
    
    :param name: name of the queue
    :param serializer: the class or module to serialize msgs with, must have
        methods or functions named ``dumps`` and ``loads``,
        `pickle <http://docs.python.org/library/pickle.html>`_ is the default,
        use ``None`` to store messages in plain text (suitable for strings,
        integers, etc)
    :param kwargs: additional kwargs to pass to :class:`Redis`, most commonly
        :attr:`host`, :attr:`port`, :attr:`db`
    """

    def __init__(self, name, serializer=pickle, **kwargs):
        self.name = name
        self.serializer = serializer
        self._redis = Redis(**kwargs)
    
    def __len__(self):
        return self._redis.llen(self.key)
    
    @property
    def key(self):
        """Return the key name used to store this queue in Redis."""
        return key_for_name(self.name)
    
    def clear(self):
        """Clear the queue of all messages, deleting the Redis key."""
        self._redis.delete(self.key)

    def _get(self, end, block=False, timeout=None):
        if block:
            if timeout is None:
                timeout = 0
            msg = getattr(self._redis, 'b{}pop'.format(end.value))(self.key, timeout=timeout)
            if msg is None:
                raise Empty("Redis queue {} was empty after {}s".format(
                    self.key, timeout
                ))
            else:
                msg = msg[1]
        else:
            msg = getattr(self._redis, '{}pop'.format(end.value))(self.key)
            if msg is None:
                raise Empty("Redis queue {} is empty".format(self.key))

        if msg is not None and self.serializer is not None:
            msg = self.serializer.loads(msg)
        return msg

    def _put(self, end, *msgs):
        if self.serializer is not None:
            msgs = map(self.serializer.dumps, msgs)
        getattr(self._redis, '{}push'.format(end.value))(self.key, *msgs)

    @abstractmethod
    def get(self, block=False, timeout=None):
        pass

    @abstractmethod
    def put(self, *msgs):
        pass

    def consume(self, limit=None, **kwargs):
        """Return a generator that yields whenever a message is waiting in the
        queue. Will block otherwise. Example:

        >>> for msg in queue.consume(timeout=1):
        ...     print msg
        my message
        another message

        :param limit: maximum number of items to retrieve
            (default ``None``, i.e. infinite)
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        kwargs.setdefault('block', True)
        limit = limit or float("inf")
        count = 0
        while count < limit:
            try:
                msg = self.get(**kwargs)
            except Empty:
                break
            yield msg
            count += 1

    def worker(self, *args, **kwargs):
        """Decorator for using a function as a queue worker. Example:
        
        >>> @queue.worker(timeout=1)
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        You can also use it without passing any keyword arguments:
        
        >>> @queue.worker
        ... def printer(msg):
        ...     print msg
        >>> printer()
        my message
        another message
        
        :param kwargs: any arguments that :meth:`~hotqueue.HotQueue.get` can
            accept (:attr:`block` will default to ``True`` if not given)
        """
        def decorator(worker):
            @wraps(worker)
            def wrapper(*args):
                for msg in self.consume(**kwargs):
                    worker(*args + (msg,))
            return wrapper
        if args:
            return decorator(*args)
        return decorator


class FifoHotQueue(BaseHotQueue):
    """First-in, first-out queue backed by a Redis list"""

    def put(self, *msgs):
        return self._put(End.BACK, *msgs)

    def get(self, block=False, timeout=None):
        return self._get(End.FRONT, block, timeout)


HotQueue = FifoHotQueue


class LifoHotQueue(BaseHotQueue):
    """Last-in, first-out queue backed by a Redis list"""

    def put(self, *msgs):
        return self._put(End.FRONT, *msgs)

    def get(self, block=False, timeout=None):
        return self._get(End.FRONT, block, timeout)


class DeHotQueue(BaseHotQueue):
    """
    Double-ended queue backed by a Redis list.
    By default, ``get``, ``put``, ``consume``, and ``worker`` act
    like a FifoHotQueue (putting on the back, getting from the front),
    but the put_end and get_end properties can be changed.
    """

    _get_end = End.FRONT
    _put_end = End.BACK

    @property
    def get_end(self):
        return self._get_end

    @get_end.setter
    def get_end(self, val):
        self._get_end = End.from_str(val)

    @property
    def put_end(self):
        return self._put_end

    @put_end.setter
    def put_end(self, val):
        self._put_end = End.from_str(val)

    def put_front(self, *msgs):
        return self._put(End.FRONT, *msgs)

    def put_back(self, *msgs):
        return self._put(End.BACK, *msgs)

    def put(self, *msgs):
        return self._put(self.put_end, *msgs)

    def get_front(self, block=False, timeout=None):
        return self._get(End.FRONT, block, timeout)

    def get_back(self, block=False, timeout=None):
        return self._get(End.BACK, block, timeout)

    def get(self, block=False, timeout=None):
        return self._get(self.get_end, block, timeout)
