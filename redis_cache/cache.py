"""
  `Cache`
--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--·--
   2016 Jared Lunde © The MIT License (MIT)
   http://github.com/jaredlunde/redis-cache
"""
import time
import datetime
from traceback import format_exc
from functools import wraps
from collections import OrderedDict

try:
    import ujson as json
except ImportError:
    import json

from vital.cache.decorators import high_pickle, cached_property
from vital.debug import preprX, line, format_obj_name
from vital.tools.lists import pairwise, flatten

from redis import StrictRedis
from redis_structures import RedisMap
from redis_lock import Lock


__all__ = 'StrictRedis', 'Cache', 'BaseCache'


class BaseCache(object):
    @cached_property
    def _client(self):
        """ Lazy loads the client connection """
        client = self._client_conn or StrictRedis(**self._client_config)
        if not self.encoding:
            conn = client.connection_pool.get_connection("")
            self.encoding = conn.encoding
            client.connection_pool.release(conn)
        return client

    __repr__ = preprX('key_prefix', 'serialiazed', '_ttl')

    def lock(self, name):
        """ Used as a context manager to avoid the dog-pile effect.
            ..
            rc = Cache()
            result = None
            with rc.lock('get:some-key'):
                result = rc.get('some-key')
                if not result:
                    with rc.lock('set:some-key'):
                        result = rc.get('some-key')
                        if not result:
                            #: Do some expensive task here
                            result = get_some_expensive_data()
                            rc.set('some-key', result)
            ..
        """
        keyname = '{}:{}'.format(self.key_prefix, name)
        return Lock(self._client, keyname,  expire=60, auto_renewal=True)

    def read_lock(self, name):
        """ Used as a context manager to avoid the dog-pile effect.
            ..
                result = None
                with rc.read_lock('some-key'):
                    result = rc.get('some-key')
            ..
        """
        return self.lock(name + ':read')

    def write_lock(self, name):
        """ Used as a context manager to avoid the dog-pile effect.
            ..
                with rc.write_lock('some-key'):
                    result = rc.get('some-key')
                    if not result:
                        result = get_some_expensive_data()
                        rc.set('some-key', result)
                return result
            ..
        """
        return self.lock(name + ':write')


class Cache(RedisMap, BaseCache):
    """ =======================================================================
        ``Usage Example``
        ..
            from redis_cache import StrictRedis, Cache

            #: Specialized cache
            client = StrictRedis()

            cache = Cache(name="1", prefix="redis-cache:bucket", client=client)
            print(cache.name)
            # -> '1'
            print(cache.prefix)
            # -> 'redis-cache:bucket'
            print(cache.key_prefix)
            # -> 'redis-cache:bucket:1'
            print(cache.get_key("test"))
            # -> 'redis-cache:bucket:1:test'

            #: Cache with default settings
            cache2 = Cache('2', prefix="redis-cache:bucket")
            print(cache2.name)
            # -> '2'
            print(cache2.key_prefix)
            # -> 'redis-cache:bucket:2'

            cache["my_key"] = "my value"
            # -> setex redis-cache:bucket:1:my_key my_value 300
            print(cache["my_key"])
            # -> 'my value'

            @cache.keep(500)
            def expensive_db_call():
                return db.expensive()
        ..
    """
    def __init__(self, name='1', prefix="redis-cache:bucket", ttl=300,
                 serializer=None, serialize=True, client=None,
                 save_empty=False, decode_responses=True, encoding=None,
                 **redis_config):
        """`Redis Cache`
            @name: (#str) unique name of the specific to the structure within
                @prefix, this gets appended to the eventual full redis key,
                i.e. |prefix:name:specific_key| for most structures
            @prefix: (#str) the prefix to use for your redis keys.
            @ttl: (#int) default ttl for this cache instances
            @serializer: optional serializer to use for your data before
                posting to your redis database. Must have a dumps and loads
                callable. :module:json is the default serializer
            @serialize: (#bool) True if you wish to serialize your data. This
                doesn't have to be set if @serializer is passed as an argument.
            @client: (:class:redis.StrictRedis or :class:redis.Redis)
            @save_empty: (#bool) True if the cache should store empty values,
                that is, if the length is zero or the value is |None|. By
                default empty values are not saved.
            @decode_responses: (#bool) whether or not to decode response
                keys and values from #bytes to #str
            @encoding: (#str) encoding to @decode_responses with
            @**redis_config: keyword arguments to pass to
                :class:redis.StrictRedis if no @client is supplied
        """
        #: For map
        self.name = name
        self.prefix = prefix.rstrip(":")
        self.serialized = (True if serializer is not None else False) or \
            serialize
        if serializer:
            self.serializer = serializer
        else:
            self.serializer = None if not self.serialized else json
        self._client_conn = client
        self._client_config = redis_config
        self._default = None
        self.encoding = encoding or 'utf-8'
        self.decode_responses = decode_responses

        #: For cache
        self._ttl = ttl
        self.save_empty = save_empty

    def __setitem__(self, key, value):
        """ Set cache["key"] = value, persists to Redis right away with
            the default :prop:_ttl
        """
        return self.setex(key, value, self._ttl)

    def __getitem__(self, key):
        """ Get cache["key"] from local dict if in local dict,
            otherwise from remote Redis instance
        """
        return self.get(str(key))

    def __delitem__(self, key):
        """ Deletes cache["key"] """
        return super().__delitem__(str(key))

    def _skip(self, value):
        return (value is None or
                (hasattr(value, '__len__') and not len(value)))\
               and not self.save_empty

    def setex(self, key, value, ttl=0):
        """ Sets the key and value pair to redis with given @ttl """
        if self._skip(value):
            return 0
        with self.write_lock(key):
            r = self.get(key)
            if r is None:
                r = super().setex(str(key), value, ttl or self._ttl)
            else:
                r = None
        return r

    def set(self, *kvs, ttl=0, **kwvs):
        """ Set multiple key/value pairs with the same ttl

            @*kvs: |key, value, key2, value2, ...|
            @ttl: #int time to live in seconds
            @**kwvs: |key1=value1, key2=value2|

            Note:
            |@*kvs| and |@**kwvs| cannot be intermixed, |@**kwvs| will be
            favored
            ..
                cache.set("key1", "val1", "key2", "val2")
                cache.get("key1", "key2")
                # -> ['val1', 'val2']

                cache.set(key1="val1", key2="val2")
                cache.get("key1", "key2")
                # -> ['val1', 'val2']
            ..
        """
        ttl = ttl or self._ttl
        if len(kvs) > 2 or kwvs:
            # Pipeline set
            pipe = self._client.pipeline(transaction=False)
            setex = pipe.setex
            for k, v in (kwvs.items() if kwvs else pairwise(kvs)):
                if not self._skip(v):
                    k = str(k)
                    setex(self.get_key(k), ttl, self._dumps(v))
            result = pipe.execute()
            return result
        else:
            k, v = kvs
            return self.setex(k, v, ttl)

    def update(self, kvs, ttl=0):
        """ Set multiple key/value pairs

            @kvs: #dict of key value pairs or key: (value, ttl) tuples
                |{k: (v, ttl), k: v}| - if (value, ttl) is not set as the value
                @ttl will be used, if no @ttl is given, :prop:_ttl will be used
                by default
            @ttl: #int time to live in seconds
            ..
                cache = Cache(ttl=100)
                cache.update({
                    "key1": "val1",
                    "key2": "val2",
                    "key3": "val3"
                })
                cache.get("key1", "key2", "key3")
                # -> ['val1', 'val2', 'val3']

                time.sleep(1)

                cache.get_ttl('key2')
                # -> 99
            ..
        """
        ttl = ttl or self._ttl
        try:
            kvs = kvs.items()
        except AttributeError:
            kvs = kvs
        return self.set(*flatten(kvs), ttl=ttl)

    def get(self, *keys, default=None):
        """ REMOTELY get key / value pairs by @keys
            ..
                cache.get("key1", "key2")
                # -> ['val1', 'val2']

                cache.get("key1")
                # -> 'val1'
            ..
        """
        if len(keys) > 1:
            # Pipeline get
            results = self.mget(*keys)
            if default:
                return [r if r is not None else default for r in results]
            return results
        elif keys:
            try:
                return super().__getitem__(str(keys[0]))
            except KeyError:
                return default
        else:
            return default

    def keep(self, ttl=None, prefix=None, serialize_args=True):
        """ Redis-backed memoizer and simple caching utility

            @ttl: (#int) time to live in seconds
            @prefix: (#str) prefix overrides local :prop:RedisMap.prefix
            @serialize_args: (#bool) whether or not to serialize the
                wrapped function's arguments with :prop:RedisMap.serializer.
                If set to False, it will str((args, kwargs)) instead
            ..
                #: Must derive from Cache instance
                cache = Cache(name="json", serializer=json)

                @cache.keep(ttl=300)
                def expensive_db_func(*args, **kwargs):
                    pass

                exensive_db_func("fun", times="to be had")
                #: Resulting key:
                #  your_prefix:and_name:expensive_db_func: \\
                #      [["fun"],{"times":"to be had"}]
            ..
        """
        serializer = self._dumps if self._dumps and self.serialized \
            else json.dumps
        prefix = "" if not prefix else prefix.rstrip(":") + ":"

        def keeper(obj):
            @wraps(obj)
            def memoizer(*args, **kwargs):
                argkey = serializer((args, kwargs)) if serialize_args \
                    else str((args, kwargs))
                key = "{}{}:{}".format(prefix, format_obj_name(obj), argkey)
                r = None
                try:
                    r = super(Cache, self).__getitem__(key)
                except KeyError:
                    with self.write_lock(key):
                        r = self.get(key)
                        if r is None:
                            r = obj(*args, **kwargs)
                            if not self._skip(r):
                                super(Cache, self).setex(key,
                                                         r,
                                                         ttl or self._ttl)
                return r
            return memoizer
        return keeper

    #: Aliases
    add = set
    delete = RedisMap.remove
    flush = RedisMap.clear
