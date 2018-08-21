# -*- coding: utf-8 -*-

"""Test suite for the HotQueue library. To run this test suite, execute this
Python program (``python tests.py``). Redis must be running on localhost:6379,
and a list key named 'hotqueue:testqueue' will be created and deleted in db 0
several times while the tests are running.
"""

from __future__ import unicode_literals

import sys
import random

try:
    import queue
except ImportError:
    import Queue as queue
from time import sleep
import threading
import unittest
try:
    import cPickle as pickle
except ImportError:
    import pickle

from hotqueue import HotQueue, LifoHotQueue, DeHotQueue


class DummySerializer(object):
    """Dummy serializer that deliberately discards messages on dumps."""
    @staticmethod
    def dumps(s):
        return "foo"

    @staticmethod
    def loads(s):
        return s


class FifoHotQueueTestCase(unittest.TestCase):
    ClassUnderTest = HotQueue
    
    def setUp(self):
        """Create the queue instance before the test."""
        self.queue = self.ClassUnderTest(
            'testqueue-{}-{:05}'.format(self.id(), random.randint(0, 99999))
        )
    
    def tearDown(self):
        """Clear the queue after the test."""
        self.queue.clear()
    
    def test_arguments(self):
        """Test that HotQueue.__init__ accepts arguments correctly, and that
        the Redis key is correctly formed.
        """
        kwargs = {
            'name': "testqueue",
            'serializer': DummySerializer,
            'host': "localhost",
            'port': 6379,
            'db': 0}
        # Instantiate the HotQueue instance:
        self.queue = HotQueue(**kwargs)
        # Ensure that the properties of the instance are as expected:
        self.assertEqual(self.queue.name, kwargs['name'])
        self.assertEqual(self.queue.key, "hotqueue:%s" % kwargs['name'])
        self.assertEqual(self.queue.serializer, kwargs['serializer'])
        # Instantiate a HotQueue instance with only the required args:
        self.queue = HotQueue(kwargs['name'])
        # Ensure that the properties of the instance are as expected:
        self.assertEqual(self.queue.name, kwargs['name'])
        self.assertEqual(self.queue.key, "hotqueue:%s" % kwargs['name'])
        self.assertTrue(self.queue.serializer is pickle) # Defaults to cPickle
                                                         # or pickle, depending
                                                         # on the platform.

    def test_cleared(self):
        """Test for correct behaviour if the Redis list does not exist."""
        self.assertEqual(len(self.queue), 0)
        with self.assertRaises(queue.Empty):
            self.queue.get()
    
    def test_length(self):
        """Test that the length of a queue is returned correctly."""
        self.queue.put('a message')
        self.queue.put('another message')
        self.assertEqual(len(self.queue), 2)

    def test_custom_serializer(self):
        """Test the use of a custom serializer and None as serializer."""
        msg = "my message"
        # Test using None:
        self.queue.serializer = None
        self.queue.put(msg)
        self.assertEqual(self.queue.get().decode(), msg)

        self.queue.put({"a": 1})
        expected = "{u'a': 1}" if sys.version_info[0] == 2 else "{'a': 1}"
        self.assertEqual(self.queue.get().decode(), expected)  # Should be a string
        # Test using DummySerializer:
        self.queue.serializer = DummySerializer
        self.queue.put(msg)
        self.assertEqual(self.queue.get().decode(), "foo")

    def check_correct_get_order(self, reverse=False):
        alphabet = ['abc', 'def', 'ghi', 'jkl', 'mno']
        self.queue.put(alphabet[0], alphabet[1], alphabet[2])
        self.queue.put(alphabet[3])
        self.queue.put(alphabet[4])
        msgs = []
        msgs.append(self.queue.get())
        msgs.append(self.queue.get())
        msgs.append(self.queue.get())
        msgs.append(self.queue.get())
        msgs.append(self.queue.get())
        expected = alphabet[::-1] if reverse else alphabet
        self.assertEqual(msgs, expected)

    def test_get_order(self):
        """Test that messages are get in the same order they are put."""
        self.check_correct_get_order()

    def check_consume(self, reverse=False):
        """Test the consume generator method."""
        nums = [1, 2, 3, 4, 5, 6, 7, 8]
        expected = nums[::-1] if reverse else nums
        # Test blocking with timeout:
        self.queue.put(*nums)
        msgs = []
        for msg in self.queue.consume(timeout=1):
            msgs.append(msg)
        self.assertEqual(msgs, expected)
        # Test non-blocking:
        self.queue.put(*nums)
        msgs = []
        for msg in self.queue.consume(block=False):
            msgs.append(msg)
        self.assertEqual(msgs, expected)

    def test_consume(self):
        self.check_consume()
    
    def check_worker(self, reverse=False):
        """Test the worker decorator."""
        colors = ['blue', 'green', 'red', 'pink', 'black']
        expected = colors[::-1] if reverse else colors
        # Test blocking with timeout:
        self.queue.put(*colors)
        msgs = []
        @self.queue.worker(timeout=1)
        def appender(msg):
            msgs.append(msg)
        appender()
        self.assertEqual(msgs, expected)
        # Test non-blocking:
        self.queue.put(*colors)
        msgs = []
        @self.queue.worker(block=False)
        def appender(msg):
            msgs.append(msg)
        appender()
        self.assertEqual(msgs, expected)
        # Test decorating a class method:
        self.queue.put(*colors)
        msgs = []
        class MyClass(object):
            @self.queue.worker(block=False)
            def appender(self, msg):
                msgs.append(msg)
        my_instance = MyClass()
        my_instance.appender()
        self.assertEqual(msgs, expected)

    def test_worker(self):
        self.check_worker()
    
    def test_threaded(self):
        """Threaded test of put and consume methods."""
        msgs = []
        def put():
            for num in range(3):
                self.queue.put('message %d' % num)
                sleep(0.1)
        def consume():
            for msg in self.queue.consume(timeout=1):
                msgs.append(msg)
        putter = threading.Thread(target=put)
        consumer = threading.Thread(target=consume)
        putter.start()
        consumer.start()
        for thread in [putter, consumer]:
            thread.join()

        self.assertEqual(set(msgs), {"message 0", "message 1", "message 2"})


class LifoHotQueueTestCase(FifoHotQueueTestCase):
    ClassUnderTest = LifoHotQueue

    def test_get_order(self):
        self.check_correct_get_order(True)

    def test_consume(self):
        self.check_consume(True)

    def test_worker(self):
        self.check_worker(True)


class DeHotQueueTestCase(FifoHotQueueTestCase):
    ClassUnderTest = DeHotQueue

    def switch_ends(self, get=True, put=True):
        if get:
            self.queue.get_end = self.queue.get_end.opposite()
        if put:
            self.queue.put_end = self.queue.put_end.opposite()

    def check_with_switches(self, method):
        method()
        self.switch_ends()
        method()
        self.switch_ends(put=False)
        method(reverse=True)
        self.switch_ends()
        method(reverse=True)

    def test_get_order(self):
        self.check_with_switches(self.check_correct_get_order)

    def test_consume(self):
        self.check_with_switches(self.check_consume)

    def test_worker(self):
        self.check_with_switches(self.check_worker)


if __name__ == "__main__":
    unittest.main()

