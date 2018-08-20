# -*- coding: utf-8 -*-

"""HotQueue is a Python library that allows you to use Redis as a message queue
within your Python programs.
"""

from __future__ import unicode_literals
from functools import wraps
from queue import Empty

try:
    import cPickle as pickle
except ImportError:
    import pickle

from redis import Redis


__all__ = ['HotQueue']

__version__ = '0.2.8'
__version_info__ = tuple(int(v) for v in __version__.split('.'))


def key_for_name(name):
    """Return the key name used to store the given queue name in Redis."""
    return 'hotqueue:{}'.format(name)


class HotQueue(object):
    
    """Simple FIFO message queue stored in a Redis list. Example:

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
    
    def get(self, block=False, timeout=None):
        """Return a message from the queue. Example:
    
        >>> queue.get()
        'my message'
        >>> queue.get()
        'another message'
        
        :param block: whether or not to wait until a msg is available in
            the queue; ``False`` by default. Will raise ``queue.Empty`` if
            no message is available
        :param timeout: when using :attr:`block`, if no msg is available
            for :attr:`timeout` in seconds, raise ``queue.Empty``
        """
        if block:
            if timeout is None:
                timeout = 0
            msg = self._redis.blpop(self.key, timeout=timeout)
            if msg is None:
                raise Empty("Redis queue {} was empty after {}s".format(
                    self.key, timeout
                ))
            else:
                msg = msg[1]
        else:
            msg = self._redis.lpop(self.key)
            if msg is None:
                raise Empty("Redis queue {} is empty".format(self.key))

        if msg is not None and self.serializer is not None:
            msg = self.serializer.loads(msg)
        return msg
    
    def put(self, *msgs):
        """Put one or more messages onto the queue. Example:
        
        >>> queue.put("my message")
        >>> queue.put("another message")
        
        To put messages onto the queue in bulk, which can be significantly
        faster if you have a large number of messages:
        
        >>> queue.put("my message", "another message", "third message")
        """
        if self.serializer is not None:
            msgs = map(self.serializer.dumps, msgs)
        self._redis.rpush(self.key, *msgs)
    
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

