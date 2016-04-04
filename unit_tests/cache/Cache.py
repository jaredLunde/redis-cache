#!/usr/bin/python3 -S
# -*- coding: utf-8 -*-
import os
import sys
import time

import redis_lock
from vital.debug import RandData

cd = os.path.dirname(os.path.abspath(__file__))
path = cd.split('redis-cache')[0] + 'redis-cache'
sys.path.insert(0, path)

from unit_tests import configure


class TestCache(configure.BaseTestCase):

    def test_init(self):
        pass

    def test___setitem__(self):
        self.cache['test'] = 'best'
        self.assertEqual(self.cache['test'], 'best')

    def test___getitem__(self):
        self.cache['test'] = 'best'
        self.assertEqual(self.cache['test'], 'best')
        self.assertIsNone(self.cache['foo'])

    def test___delitem__(self):
        self.cache['test'] = 'best'
        self.assertEqual(self.cache['test'], 'best')
        del self.cache['test']
        self.assertIsNone(self.cache['test'])

    def test_lock(self):
        with self.cache.lock('get:some-key'):
            result = self.cache.get('some-key')
            self.assertIsNone(result)
            if not result:
                with self.cache.lock('set:some-key'):
                    result = self.cache.get('some-key')
                    self.assertIsNone(result)
                    if not result:
                        #: Do some expensive task here
                        result = 'foobar'
                        self.assertTrue(self.cache.set('some-key', result))

    def test_get_lock(self):
        lock = self.cache.get_lock('some-key')
        self.assertIsInstance(lock, redis_lock.Lock)
        self.assertEqual(lock._name,
                         'lock:redis-cache:bucket:1:some-key:read')

    def test_set_lock(self):
        lock = self.cache.set_lock('some-key')
        self.assertIsInstance(lock, redis_lock.Lock)
        self.assertEqual(lock._name,
                         'lock:redis-cache:bucket:1:some-key:write')

    def test_setex(self):
        self.assertTrue(self.cache.setex('foo', 'bar', 1))
        time.sleep(1.0)
        self.assertIsNone(self.cache['foo'])
        self.assertEqual(self.cache.setex('foo', None, 1), 0)
        self.assertEqual(self.cache.setex('foo', '', 1), 0)

    def test_set(self):
        for result in self.cache.set('foo', 'bar', 'foo2', 'bar2',
                                     'foo3', 'bar3'):
            self.assertTrue(result)
        self.assertTrue(self.cache.set('foo4', 'bar4'))
        for result in self.cache.set(foo5='bar5', foo6='bar6', foo7='bar7',
                                     ttl=50):
            self.assertTrue(result)
        self.assertGreater(self.cache.ttl('foo5'), 40)

    def test_update(self):
        kvs = {
            'foo': 'bar',
            'foo2': 'bar2',
            'foo3': 'bar3'
        }
        for result in self.cache.update(kvs):
            self.assertTrue(result)
        for result in self.cache.update(kvs.items()):
            self.assertTrue(result)
        self.cache.update(kvs, ttl=3000)
        self.assertGreater(self.cache.ttl('foo'), 2990)

    def test_get(self):
        self.cache.set('foo', 'bar', 'foo2', 'bar2', 'foo3', 'bar3')
        self.assertListEqual(self.cache.get('foo', 'foo2', 'foo3'),
                             ['bar', 'bar2', 'bar3'])
        self.assertListEqual(self.cache.get('foo4', 'foo5', 'foo6'),
                             [None, None, None])
        self.assertListEqual(self.cache.get('foo4', 'foo5', 'foo6',
                                            default='bar'),
                             ['bar', 'bar', 'bar'])

    def test_keep(self):
        @self.cache.keep(100)
        def expensive_func(val):
            return val * 1000
        self.assertEqual(expensive_func(5), 5000)
        self.assertEqual(expensive_func(5), 5000)
        self.assertEqual(expensive_func(6), 6000)

    def test_unserialized_keep(self):
        @self.cache.keep(100, serialize_args=False)
        def expensive_func_unserialized(val):
            return val * 1000
        self.assertEqual(expensive_func_unserialized(5), 5000)
        self.assertEqual(expensive_func_unserialized(5), 5000)
        self.assertEqual(expensive_func_unserialized(6), 6000)

    def test_prefixed_keep(self):
        @self.cache.keep(10, prefix='foo')
        def expensive_func_prefixed(val):
            return val * 1000
        self.assertEqual(expensive_func_prefixed(5), 5000)
        self.assertEqual(expensive_func_prefixed(5), 5000)
        self.assertEqual(expensive_func_prefixed(6), 6000)

    def test_flush(self):
        d = RandData(int).dict(10000, 1)
        for result in self.cache.update(d, ttl=300):
            self.assertTrue(result)
        for result in self.cache.get(*d.keys()):
            self.assertIsNotNone(result)
        self.cache.flush()
        for result in self.cache.get(*d.keys()):
            self.assertIsNone(result)


class TestPickleCache(TestCache):
    cache = configure.BaseTestCase.pickle_cache


class TestUnserializedCache(TestCache):
    cache = configure.BaseTestCase.plain_cache

    def test_keep(self):
        @self.cache.keep(100)
        def expensive_func(val):
            return val * 1000
        self.assertEqual(expensive_func(5), 5000)
        self.assertEqual(expensive_func(5), '5000')
        self.assertEqual(expensive_func(6), 6000)

    def test_unserialized_keep(self):
        @self.cache.keep(100)
        def expensive_func_unserialized(val, serialize_args=False):
            return val * 1000
        self.assertEqual(expensive_func_unserialized(5), 5000)
        self.assertEqual(expensive_func_unserialized(5), '5000')
        self.assertEqual(expensive_func_unserialized(6), 6000)

    def test_prefixed_keep(self):
        @self.cache.keep(100, prefix='foo')
        def expensive_func_prefixed(val):
            return val * 1000
        self.assertEqual(expensive_func_prefixed(5), 5000)
        self.assertEqual(expensive_func_prefixed(5), '5000')
        self.assertEqual(expensive_func_prefixed(6), 6000)


if __name__ == '__main__':
    configure.run_tests(TestCache,
                        TestPickleCache,
                        TestUnserializedCache,
                        verbosity=2,
                        failfast=True)
