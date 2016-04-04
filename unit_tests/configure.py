import os
import unittest

from vital.cache.decorators import high_pickle

from redis_cache import Cache


def run_tests(*tests, **opts):
    suite = unittest.TestSuite()
    for test_class in tests:
        tests = unittest.defaultTestLoader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    ut = unittest.TextTestRunner(**opts)
    return ut.run(suite)


def run_discovered(path=None):
    path = path or os.path.dirname(os.path.realpath(__file__))
    ut = unittest.TextTestRunner(verbosity=2, failfast=True)
    tests = []
    suite = unittest.TestSuite()
    for test in unittest.defaultTestLoader.discover(
            path, pattern='*.py', top_level_dir=None):
        suite.addTests((t for t in test
                        if t not in tests and not tests.append(t)))
    return ut.run(suite)


def setup(cls):
    cls.cache.clear()
    cls.pickle_cache.clear()
    cls.plain_cache.clear()


def cleanup(cls):
    cls.cache.clear()
    cls.pickle_cache.clear()
    cls.plain_cache.clear()


class BaseTestCase(unittest.TestCase):
    cache = Cache()
    pickle_cache = Cache(serializer=high_pickle)
    plain_cache = Cache(serialize=False)

    def setup():
        setup(self)

    def teardown():
        cleanup(self)

    @staticmethod
    def tearDownClass():
        cleanup(BaseTestCase)
