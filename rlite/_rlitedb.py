# -*- coding:utf-8 -*-

"""
Rlite操作pythonic的方式
"""
import datetime
from itertools import imap, izip
from multiprocessing import TimeoutError

import hirlite

from _compat import iteritems, iterkeys, itervalues


def list_or_args(keys, args):
    # returns a single list combining keys and args
    try:
        iter(keys)
        # a string or bytes instance can be iterated, but indicates
        # keys wasn't passed as a list
        if isinstance(keys, (basestring, bytes)):
            keys = [keys]
    except TypeError:
        keys = [keys]
    if args:
        keys.extend(args)
    return keys


class Token(object):
    """
    Literal strings in Redis commands, such as the command names and any
    hard-coded arguments are wrapped in this class so we know not to apply
    and encoding rules on them.
    """

    def __init__(self, value):
        if isinstance(value, Token):
            value = value.value
        self.value = value

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


class ORDER(object):
    # keys
    EXISTS = "EXISTS"
    DEL = "DEL"

    # strings
    APPEND = "APPEND"
    BITCOUNT = "BITCOUNT"
    BITFIELD = "BITFIELD"
    BITOP = "BITOP"
    Perform = "Perform"
    BITPOS = "BITPOS"
    Find = "Find"
    DECR = "DECR"
    DECRBY = "DECRBY"
    GET = "GET"
    GETBIT = "GETBIT"
    GETRANGE = "GETRANGE"
    GETSET = "GETSET"
    INCR = "INCR"
    INCRBY = "INCRBY"
    INCRBYFLOAT = "INCRBYFLOAT"
    MGET = "MGET"
    MSET = "MSET"
    MSETNX = "MSETNX"
    PSETEX = "PSETEX"
    Set = "Set"
    SET = "SET"
    SETBIT = "SETBIT"
    SETEX = "SETEX"
    SETNX = "SETNX"
    SETRANGE = "SETRANGE"
    STRLEN = "STRLEN"

    # list
    BLPOP = "BLPOP"
    BRPOP = "BRPOP"
    BRPOPLPUSH = "BRPOPLPUSH"
    LINDEX = "LINDEX"
    LINSERT = "LINSERT"
    BEFORE = "BEFORE"
    AFTER = "AFTER"
    LLEN = "LLEN"
    LPOP = "LPOP"
    LPUSH = "LPUSH"
    LPUSHX = "LPUSHX"
    LRANGE = "LRANGE"
    LREM = "LREM"
    LSET = 'LSET'
    LTRIM = "LTRIM"
    RPOP = "RPOP"
    RPOPLPUSH = "RPOPLPUSH"
    RPUSH = "RPUSH"
    RPUSHX = "RPUSHX"

    # hash
    HDEL = "HDEL"
    HEXISTS = "HEXISTS"
    HGET = "HGET"
    HGETALL = "HGETALL"
    HINCRBY = "HINCRBY"
    HINCRBYFLOAT = "HINCRBYFLOAT"
    HKEYS = "HKEYS"
    HLEN = "HLEN"
    HMGET = "HMGET"
    HMSET = "HMSET"
    HSET = "HSET"
    HSETNX = "HSETNX"
    HSTRLEN = "HSTRLEN"
    HVALS = "HVALS"
    HSCAN = "HSCAN"


class RliteDB(object):
    def __init__(self, db_name="test"):
        self.db_name = db_name
        self.db = hirlite.Rlite(path=self.db_name)

    def command(self, order, *args, **kwargs):
        return self.db.command(order, *args, **kwargs)

    def echo(self, value):
        return self.command('ECHO', value)

    def ping(self):
        return self.command('PING')

    def time(self):
        return self.command('TIME')

    def wait(self, num_replicas, timeout):
        return self.command('WAIT', num_replicas, timeout)

    def append(self, key, value):
        return self.command('APPEND', key, value)

    def bitcount(self, key, start=None, end=None):
        return self.command('BITCOUNT', key, start, end)

    def bitop(self, operation, dest, *keys):
        return self.command('BITOP', operation, dest, *keys)

    def bitpos(self, key, bit, start=None, end=None):
        return self.command('BITPOS', key, bit, start, end)

    def decr(self, name, amount=1):
        return self.command('DECRBY', name, amount)

    def delete(self, *names):
        return self.command('DEL', *names)

    def __delitem__(self, name):
        self.delete(name)

    def dump(self, name):
        return self.command('DUMP', name)

    def exists(self, name):
        return self.command('EXISTS', name)

    __contains__ = exists

    def expire(self, name, time):
        return self.command('EXPIRE', name, time)

    def expireat(self, name, when):
        return self.command('EXPIREAT', name, when)

    def get(self, name):
        return self.command('GET', name)

    def __getitem__(self, name):
        """
        Return the value at key ``name``, raises a KeyError if the key
        doesn't exist.
        """
        value = self.get(name)
        if value is not None:
            return value
        raise KeyError(name)

    def getbit(self, name, offset):
        "Returns a boolean indicating the value of ``offset`` in ``name``"
        return self.command('GETBIT', name, offset)

    def getrange(self, key, start, end):
        """
        Returns the substring of the string value stored at ``key``,
        determined by the offsets ``start`` and ``end`` (both are inclusive)
        """
        return self.command('GETRANGE', key, start, end)

    def getset(self, name, value):
        """
        Sets the value at key ``name`` to ``value``
        and returns the old value at key ``name`` atomically.
        """
        return self.command('GETSET', name, value)

    def incr(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """
        return self.command('INCRBY', name, amount)

    def incrby(self, name, amount=1):
        """
        Increments the value of ``key`` by ``amount``.  If no key exists,
        the value will be initialized as ``amount``
        """

        # An alias for ``incr()``, because it is already implemented
        # as INCRBY redis command.
        return self.incr(name, amount)

    def incrbyfloat(self, name, amount=1.0):
        """
        Increments the value at key ``name`` by floating ``amount``.
        If no key exists, the value will be initialized as ``amount``
        """
        return self.command('INCRBYFLOAT', name, amount)

    def keys(self, pattern='*'):
        "Returns a list of keys matching ``pattern``"
        return self.command('KEYS', pattern)

    def mget(self, keys, *args):
        """
        Returns a list of values ordered identically to ``keys``
        """
        args = list_or_args(keys, args)
        return self.command('MGET', *args)

    def mset(self, *args, **kwargs):
        """
        Sets key/values based on a mapping. Mapping can be supplied as a single
        dictionary argument or as kwargs.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise Exception('MSET requires **kwargs or a single dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.command('MSET', *items)

    def msetnx(self, *args, **kwargs):
        """
        Sets key/values based on a mapping if none of the keys are already set.
        Mapping can be supplied as a single dictionary argument or as kwargs.
        Returns a boolean indicating if the operation was successful.
        """
        if args:
            if len(args) != 1 or not isinstance(args[0], dict):
                raise Exception('MSETNX requires **kwargs or a single ' 'dict arg')
            kwargs.update(args[0])
        items = []
        for pair in iteritems(kwargs):
            items.extend(pair)
        return self.command('MSETNX', *items)

    def move(self, name, db):
        "Moves the key ``name`` to a different Redis database ``db``"
        return self.command('MOVE', name, db)

    def persist(self, name):
        "Removes an expiration on ``name``"
        return self.command('PERSIST', name)

    def pexpire(self, name, time):
        """
        Set an expire flag on key ``name`` for ``time`` milliseconds.
        ``time`` can be represented by an integer or a Python timedelta
        object.
        """
        if isinstance(time, datetime.timedelta):
            ms = int(time.microseconds / 1000)
            time = (time.seconds + time.days * 24 * 3600) * 1000 + ms
        return self.command('PEXPIRE', name, time)

    def pexpireat(self, name, when):
        """
        Set an expire flag on key ``name``. ``when`` can be represented
        as an integer representing unix time in milliseconds (unix time * 1000)
        or a Python datetime object.
        """
        return self.command('PEXPIREAT', name, when)

    def psetex(self, name, time_ms, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time_ms``
        milliseconds. ``time_ms`` can be represented by an integer or a Python
        timedelta object
        """
        if isinstance(time_ms, datetime.timedelta):
            ms = int(time_ms.microseconds / 1000)
            time_ms = (time_ms.seconds + time_ms.days * 24 * 3600) * 1000 + ms
        return self.command('PSETEX', name, time_ms, value)

    def pttl(self, name):
        "Returns the number of milliseconds until the key ``name`` will expire"
        return self.command('PTTL', name)

    def randomkey(self):
        "Returns the name of a random key"
        return self.command('RANDOMKEY')

    def rename(self, src, dst):
        """
        Rename key ``src`` to ``dst``
        """
        return self.command('RENAME', src, dst)

    def renamenx(self, src, dst):
        "Rename key ``src`` to ``dst`` if ``dst`` doesn't already exist"
        return self.command('RENAMENX', src, dst)

    def restore(self, name, ttl, value, replace=False):
        """
        Create a key using the provided serialized value, previously obtained
        using DUMP.
        """
        params = [name, ttl, value]
        if replace:
            params.append('REPLACE')
        return self.command('RESTORE', *params)

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        """
        Set the value at key ``name`` to ``value``
        ``ex`` sets an expire flag on key ``name`` for ``ex`` seconds.
        ``px`` sets an expire flag on key ``name`` for ``px`` milliseconds.
        ``nx`` if set to True, set the value at key ``name`` to ``value`` if it
            does not already exist.
        ``xx`` if set to True, set the value at key ``name`` to ``value`` if it
            already exists.
        """
        pieces = [name, value]
        if ex:
            pieces.append('EX')
            if isinstance(ex, datetime.timedelta):
                ex = ex.seconds + ex.days * 24 * 3600
            pieces.append(ex)
        if px:
            pieces.append('PX')
            if isinstance(px, datetime.timedelta):
                ms = int(px.microseconds / 1000)
                px = (px.seconds + px.days * 24 * 3600) * 1000 + ms
            pieces.append(px)

        if nx:
            pieces.append('NX')
        if xx:
            pieces.append('XX')
        return self.command('SET', *pieces)

    def __setitem__(self, name, value):
        self.set(name, value)

    def setbit(self, name, offset, value):
        """
        Flag the ``offset`` in ``name`` as ``value``. Returns a boolean
        indicating the previous value of ``offset``.
        """
        value = value and 1 or 0
        return self.command('SETBIT', name, offset, value)

    def setex(self, name, time, value):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.command('SETEX', name, time, value)

    def setnx(self, name, value):
        "Set the value of key ``name`` to ``value`` if key doesn't exist"
        return self.command('SETNX', name, value)

    def setrange(self, name, offset, value):
        """
        Overwrite bytes in the value of ``name`` starting at ``offset`` with
        ``value``. If ``offset`` plus the length of ``value`` exceeds the
        length of the original value, the new value will be larger than before.
        If ``offset`` exceeds the length of the original value, null bytes
        will be used to pad between the end of the previous value and the start
        of what's being injected.
        Returns the length of the new string.
        """
        return self.command('SETRANGE', name, offset, value)

    def strlen(self, name):
        "Return the number of bytes stored in the value of ``name``"
        return self.command('STRLEN', name)

    def substr(self, name, start, end=-1):
        """
        Return a substring of the string at key ``name``. ``start`` and ``end``
        are 0-based integers specifying the portion of the string to return.
        """
        return self.command('SUBSTR', name, start, end)

    def ttl(self, name):
        "Returns the number of seconds until the key ``name`` will expire"
        return self.command('TTL', name)

    def type(self, name):
        "Returns the type of key ``name``"
        return self.command('TYPE', name)

    # LIST COMMANDS
    def blpop(self, keys, timeout=0):
        """
        LPOP a value off of the first non-empty list
        named in the ``keys`` list.
        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.
        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.command('BLPOP', *keys)

    def brpop(self, keys, timeout=0):
        """
        RPOP a value off of the first non-empty list
        named in the ``keys`` list.
        If none of the lists in ``keys`` has a value to LPOP, then block
        for ``timeout`` seconds, or until a value gets pushed on to one
        of the lists.
        If timeout is 0, then block indefinitely.
        """
        if timeout is None:
            timeout = 0
        if isinstance(keys, basestring):
            keys = [keys]
        else:
            keys = list(keys)
        keys.append(timeout)
        return self.command('BRPOP', *keys)

    def brpoplpush(self, src, dst, timeout=0):
        """
        Pop a value off the tail of ``src``, push it on the head of ``dst``
        and then return it.
        This command blocks until a value is in ``src`` or until ``timeout``
        seconds elapse, whichever is first. A ``timeout`` value of 0 blocks
        forever.
        """
        if timeout is None:
            timeout = 0
        return self.command('BRPOPLPUSH', src, dst, timeout)

    def lindex(self, name, index):
        """
        Return the item from list ``name`` at position ``index``
        Negative indexes are supported and will return an item at the
        end of the list
        """
        return self.command('LINDEX', name, index)

    def linsert(self, name, where, refvalue, value):
        """
        Insert ``value`` in list ``name`` either immediately before or after
        [``where``] ``refvalue``
        Returns the new length of the list on success or -1 if ``refvalue``
        is not in the list.
        """
        return self.command('LINSERT', name, where, refvalue, value)

    def llen(self, name):
        "Return the length of the list ``name``"
        return self.command('LLEN', name)

    def lpop(self, name):
        "Remove and return the first item of the list ``name``"
        return self.command('LPOP', name)

    def lpush(self, name, *values):
        "Push ``values`` onto the head of the list ``name``"
        return self.command('LPUSH', name, *values)

    def lpushx(self, name, value):
        "Push ``value`` onto the head of the list ``name`` if ``name`` exists"
        return self.command('LPUSHX', name, value)

    def lrange(self, name, start, end):
        """
        Return a slice of the list ``name`` between
        position ``start`` and ``end``
        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.command('LRANGE', name, start, end)

    def lrem(self, name, count, value):
        """
        Remove the first ``count`` occurrences of elements equal to ``value``
        from the list stored at ``name``.
        The count argument influences the operation in the following ways:
            count > 0: Remove elements equal to value moving from head to tail.
            count < 0: Remove elements equal to value moving from tail to head.
            count = 0: Remove all elements equal to value.
        """
        return self.command('LREM', name, count, value)

    def lset(self, name, index, value):
        "Set ``position`` of list ``name`` to ``value``"
        return self.command('LSET', name, index, value)

    def ltrim(self, name, start, end):
        """
        Trim the list ``name``, removing all values not within the slice
        between ``start`` and ``end``
        ``start`` and ``end`` can be negative numbers just like
        Python slicing notation
        """
        return self.command('LTRIM', name, start, end)

    def rpop(self, name):
        "Remove and return the last item of the list ``name``"
        return self.command('RPOP', name)

    def rpoplpush(self, src, dst):
        """
        RPOP a value off of the ``src`` list and atomically LPUSH it
        on to the ``dst`` list.  Returns the value.
        """
        return self.command('RPOPLPUSH', src, dst)

    def rpush(self, name, *values):
        "Push ``values`` onto the tail of the list ``name``"
        return self.command('RPUSH', name, *values)

    def rpushx(self, name, value):
        "Push ``value`` onto the tail of the list ``name`` if ``name`` exists"
        return self.command('RPUSHX', name, value)

    def sort(self, name, start=None, num=None, by=None, get=None,
             desc=False, alpha=False, store=None, groups=False):
        """
        Sort and return the list, set or sorted set at ``name``.
        ``start`` and ``num`` allow for paging through the sorted data
        ``by`` allows using an external key to weight and sort the items.
            Use an "*" to indicate where in the key the item value is located
        ``get`` allows for returning items from external keys rather than the
            sorted data itself.  Use an "*" to indicate where int he key
            the item value is located
        ``desc`` allows for reversing the sort
        ``alpha`` allows for sorting lexicographically rather than numerically
        ``store`` allows for storing the result of the sort into
            the key ``store``
        ``groups`` if set to True and if ``get`` contains at least two
            elements, sort will return a list of tuples, each containing the
            values fetched from the arguments to ``get``.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise Exception("``start`` and ``num`` must both be specified")

        pieces = [name]
        if by is not None:
            pieces.append(Token.get_token('BY'))
            pieces.append(by)
        if start is not None and num is not None:
            pieces.append(Token.get_token('LIMIT'))
            pieces.append(start)
            pieces.append(num)
        if get is not None:
            # If get is a string assume we want to get a single value.
            # Otherwise assume it's an interable and we want to get multiple
            # values. We can't just iterate blindly because strings are
            # iterable.
            if isinstance(get, basestring):
                pieces.append(Token.get_token('GET'))
                pieces.append(get)
            else:
                for g in get:
                    pieces.append(Token.get_token('GET'))
                    pieces.append(g)
        if desc:
            pieces.append(Token.get_token('DESC'))
        if alpha:
            pieces.append(Token.get_token('ALPHA'))
        if store is not None:
            pieces.append(Token.get_token('STORE'))
            pieces.append(store)

        if groups:
            if not get or isinstance(get, basestring) or len(get) < 2:
                raise Exception('when using "groups" the "get" argument '
                                'must be specified and contain at least '
                                'two keys')

        options = {'groups': len(get) if groups else None}
        return self.command('SORT', *pieces, **options)

    # SCAN COMMANDS
    def scan(self, cursor=0, match=None, count=None):
        """
        Incrementally return lists of key names. Also return a cursor
        indicating the scan position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        return self.command('SCAN', cursor, match, count)

    def scan_iter(self, match=None, count=None):
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.scan(cursor=cursor, match=match, count=count)
            for item in data:
                yield item

    def sscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return lists of elements in a set. Also return a cursor
        indicating the scan position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        return self.command('SSCAN', *pieces)

    def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.sscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data:
                yield item

    def hscan(self, name, cursor=0, match=None, count=None):
        """
        Incrementally return key/value slices in a hash. Also return a cursor
        indicating the scan position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        return self.command('HSCAN', *pieces)

    def hscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.hscan(name, cursor=cursor,
                                      match=match, count=count)
            for item in data.items():
                yield item

    def zscan(self, name, cursor=0, match=None, count=None,
              score_cast_func=float):
        """
        Incrementally return lists of elements in a sorted set. Also return a
        cursor indicating the scan position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = [name, cursor]
        if match is not None:
            pieces.extend([Token.get_token('MATCH'), match])
        if count is not None:
            pieces.extend([Token.get_token('COUNT'), count])
        options = {'score_cast_func': score_cast_func}
        return self.command('ZSCAN', *pieces, **options)

    def zscan_iter(self, name, match=None, count=None,
                   score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.
        ``match`` allows for filtering the keys by pattern
        ``count`` allows for hint the minimum number of returns
        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = self.zscan(name, cursor=cursor, match=match,
                                      count=count,
                                      score_cast_func=score_cast_func)
            for item in data:
                yield item

    # SET COMMANDS
    def sadd(self, name, *values):
        "Add ``value(s)`` to set ``name``"
        return self.command('SADD', name, *values)

    def scard(self, name):
        "Return the number of elements in set ``name``"
        return self.command('SCARD', name)

    def sdiff(self, keys, *args):
        "Return the difference of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.command('SDIFF', *args)

    def sdiffstore(self, dest, keys, *args):
        """
        Store the difference of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.command('SDIFFSTORE', dest, *args)

    def sinter(self, keys, *args):
        "Return the intersection of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.command('SINTER', *args)

    def sinterstore(self, dest, keys, *args):
        """
        Store the intersection of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.command('SINTERSTORE', dest, *args)

    def sismember(self, name, value):
        "Return a boolean indicating if ``value`` is a member of set ``name``"
        return self.command('SISMEMBER', name, value)

    def smembers(self, name):
        "Return all members of the set ``name``"
        return self.command('SMEMBERS', name)

    def smove(self, src, dst, value):
        "Move ``value`` from set ``src`` to set ``dst`` atomically"
        return self.command('SMOVE', src, dst, value)

    def spop(self, name):
        "Remove and return a random member of set ``name``"
        return self.command('SPOP', name)

    def srandmember(self, name, number=None):
        """
        If ``number`` is None, returns a random member of set ``name``.
        If ``number`` is supplied, returns a list of ``number`` random
        memebers of set ``name``. Note this is only available when running
        Redis 2.6+.
        """
        args = number and [number] or []
        return self.command('SRANDMEMBER', name, *args)

    def srem(self, name, *values):
        "Remove ``values`` from set ``name``"
        return self.command('SREM', name, *values)

    def sunion(self, keys, *args):
        "Return the union of sets specified by ``keys``"
        args = list_or_args(keys, args)
        return self.command('SUNION', *args)

    def sunionstore(self, dest, keys, *args):
        """
        Store the union of sets specified by ``keys`` into a new
        set named ``dest``.  Returns the number of keys in the new set.
        """
        args = list_or_args(keys, args)
        return self.command('SUNIONSTORE', dest, *args)

    # SORTED SET COMMANDS
    def zadd(self, name, *args, **kwargs):
        """
        Set any number of score, element-name pairs to the key ``name``. Pairs
        can be specified in two ways:
        As *args, in the form of: score1, name1, score2, name2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...
        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 1.1, 'name1', 2.2, 'name2', name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise Exception("ZADD requires an equal number of "
                                "values and scores")
            pieces.extend(args)
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.command('ZADD', name, *pieces)

    def zcard(self, name):
        "Return the number of elements in the sorted set ``name``"
        return self.command('ZCARD', name)

    def zcount(self, name, min, max):
        """
        Returns the number of elements in the sorted set at key ``name`` with
        a score between ``min`` and ``max``.
        """
        return self.command('ZCOUNT', name, min, max)

    def zincrby(self, name, value, amount=1):
        "Increment the score of ``value`` in sorted set ``name`` by ``amount``"
        return self.command('ZINCRBY', name, amount, value)

    def zinterstore(self, dest, keys, aggregate=None):
        """
        Intersect multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZINTERSTORE', dest, keys, aggregate)

    def zlexcount(self, name, min, max):
        """
        Return the number of items in the sorted set ``name`` between the
        lexicographical range ``min`` and ``max``.
        """
        return self.command('ZLEXCOUNT', name, min, max)

    def zrange(self, name, start, end, desc=False, withscores=False,
               score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in ascending order.
        ``start`` and ``end`` can be negative, indicating the end of the range.
        ``desc`` a boolean indicating whether to sort the results descendingly
        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        ``score_cast_func`` a callable used to cast the score return value
        """
        if desc:
            return self.zrevrange(name, start, end, withscores,
                                  score_cast_func)
        pieces = ['ZRANGE', name, start, end]
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.command(*pieces, **options)

    def zrangebylex(self, name, min, max, start=None, num=None):
        """
        Return the lexicographical range of values from sorted set ``name``
        between ``min`` and ``max``.
        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise Exception("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYLEX', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        return self.command(*pieces)

    def zrevrangebylex(self, name, max, min, start=None, num=None):
        """
        Return the reversed lexicographical range of values from sorted set
        ``name`` between ``max`` and ``min``.
        If ``start`` and ``num`` are specified, then return a slice of the
        range.
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise Exception("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYLEX', name, max, min]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        return self.command(*pieces)

    def zrangebyscore(self, name, min, max, start=None, num=None,
                      withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max``.
        If ``start`` and ``num`` are specified, then return a slice
        of the range.
        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        `score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise Exception("``start`` and ``num`` must both be specified")
        pieces = ['ZRANGEBYSCORE', name, min, max]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.command(*pieces, **options)

    def zrank(self, name, value):
        """
        Returns a 0-based value indicating the rank of ``value`` in sorted set
        ``name``
        """
        return self.command('ZRANK', name, value)

    def zrem(self, name, *values):
        "Remove member ``values`` from sorted set ``name``"
        return self.command('ZREM', name, *values)

    def zremrangebylex(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` between the
        lexicographical range specified by ``min`` and ``max``.
        Returns the number of elements removed.
        """
        return self.command('ZREMRANGEBYLEX', name, min, max)

    def zremrangebyrank(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with ranks between
        ``min`` and ``max``. Values are 0-based, ordered from smallest score
        to largest. Values can be negative indicating the highest scores.
        Returns the number of elements removed
        """
        return self.command('ZREMRANGEBYRANK', name, min, max)

    def zremrangebyscore(self, name, min, max):
        """
        Remove all elements in the sorted set ``name`` with scores
        between ``min`` and ``max``. Returns the number of elements removed.
        """
        return self.command('ZREMRANGEBYSCORE', name, min, max)

    def zrevrange(self, name, start, end, withscores=False,
                  score_cast_func=float):
        """
        Return a range of values from sorted set ``name`` between
        ``start`` and ``end`` sorted in descending order.
        ``start`` and ``end`` can be negative, indicating the end of the range.
        ``withscores`` indicates to return the scores along with the values
        The return type is a list of (value, score) pairs
        ``score_cast_func`` a callable used to cast the score return value
        """
        pieces = ['ZREVRANGE', name, start, end]
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.command(*pieces, **options)

    def zrevrangebyscore(self, name, max, min, start=None, num=None,
                         withscores=False, score_cast_func=float):
        """
        Return a range of values from the sorted set ``name`` with scores
        between ``min`` and ``max`` in descending order.
        If ``start`` and ``num`` are specified, then return a slice
        of the range.
        ``withscores`` indicates to return the scores along with the values.
        The return type is a list of (value, score) pairs
        ``score_cast_func`` a callable used to cast the score return value
        """
        if (start is not None and num is None) or \
                (num is not None and start is None):
            raise Exception("``start`` and ``num`` must both be specified")
        pieces = ['ZREVRANGEBYSCORE', name, max, min]
        if start is not None and num is not None:
            pieces.extend([Token.get_token('LIMIT'), start, num])
        if withscores:
            pieces.append(Token.get_token('WITHSCORES'))
        options = {
            'withscores': withscores,
            'score_cast_func': score_cast_func
        }
        return self.command(*pieces, **options)

    def zrevrank(self, name, value):
        """
        Returns a 0-based value indicating the descending rank of
        ``value`` in sorted set ``name``
        """
        return self.command('ZREVRANK', name, value)

    def zscore(self, name, value):
        "Return the score of element ``value`` in sorted set ``name``"
        return self.command('ZSCORE', name, value)

    def zunionstore(self, dest, keys, aggregate=None):
        """
        Union multiple sorted sets specified by ``keys`` into
        a new sorted set, ``dest``. Scores in the destination will be
        aggregated based on the ``aggregate``, or SUM if none is provided.
        """
        return self._zaggregate('ZUNIONSTORE', dest, keys, aggregate)

    def _zaggregate(self, command, dest, keys, aggregate=None):
        pieces = [command, dest, len(keys)]
        if isinstance(keys, dict):
            keys, weights = iterkeys(keys), itervalues(keys)
        else:
            weights = None
        pieces.extend(keys)
        if weights:
            pieces.append(Token.get_token('WEIGHTS'))
            pieces.extend(weights)
        if aggregate:
            pieces.append(Token.get_token('AGGREGATE'))
            pieces.append(aggregate)
        return self.command(*pieces)

    # HYPERLOGLOG COMMANDS
    def pfadd(self, name, *values):
        "Adds the specified elements to the specified HyperLogLog."
        return self.command('PFADD', name, *values)

    def pfcount(self, *sources):
        """
        Return the approximated cardinality of
        the set observed by the HyperLogLog at key(s).
        """
        return self.command('PFCOUNT', *sources)

    def pfmerge(self, dest, *sources):
        "Merge N different HyperLogLogs into a single one."
        return self.command('PFMERGE', dest, *sources)

    # HASH COMMANDS
    def hdel(self, name, *keys):
        "Delete ``keys`` from hash ``name``"
        return self.command('HDEL', name, *keys)

    def hexists(self, name, key):
        "Returns a boolean indicating if ``key`` exists within hash ``name``"
        return self.command('HEXISTS', name, key)

    def hget(self, name, key):
        "Return the value of ``key`` within the hash ``name``"
        return self.command('HGET', name, key)

    def hgetall(self, name):
        "Return a Python dict of the hash's name/value pairs"
        return self.command('HGETALL', name)

    def hincrby(self, name, key, amount=1):
        "Increment the value of ``key`` in hash ``name`` by ``amount``"
        return self.command('HINCRBY', name, key, amount)

    def hincrbyfloat(self, name, key, amount=1.0):
        """
        Increment the value of ``key`` in hash ``name`` by floating ``amount``
        """
        return self.command('HINCRBYFLOAT', name, key, amount)

    def hkeys(self, name):
        "Return the list of keys within hash ``name``"
        return self.command('HKEYS', name)

    def hlen(self, name):
        "Return the number of elements in hash ``name``"
        return self.command('HLEN', name)

    def hset(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name``
        Returns 1 if HSET created a new field, otherwise 0
        """
        return self.command('HSET', name, key, value)

    def hsetnx(self, name, key, value):
        """
        Set ``key`` to ``value`` within hash ``name`` if ``key`` does not
        exist.  Returns 1 if HSETNX created a field, otherwise 0.
        """
        return self.command('HSETNX', name, key, value)

    def hmset(self, name, mapping):
        """
        Set key to value within hash ``name`` for each corresponding
        key and value from the ``mapping`` dict.
        """
        if not mapping:
            raise Exception("'hmset' with 'mapping' of length 0")
        items = []
        for pair in iteritems(mapping):
            items.extend(pair)
        return self.command('HMSET', name, *items)

    def hmget(self, name, keys, *args):
        "Returns a list of values ordered identically to ``keys``"
        args = list_or_args(keys, args)
        return self.command('HMGET', name, *args)

    def hvals(self, name):
        "Return the list of values within hash ``name``"
        return self.command('HVALS', name)

    def publish(self, channel, message):
        """
        Publish ``message`` on ``channel``.
        Returns the number of subscribers the message was delivered to.
        """
        return self.command('PUBLISH', channel, message)

    def pubsub_channels(self, pattern='*'):
        """
        Return a list of channels that have at least one subscriber
        """
        return self.command('PUBSUB CHANNELS', pattern)

    def pubsub_numpat(self):
        """
        Returns the number of subscriptions to patterns
        """
        return self.command('PUBSUB NUMPAT')

    def pubsub_numsub(self, *args):
        """
        Return a list of (channel, number of subscribers) tuples
        for each channel given in ``*args``
        """
        return self.command('PUBSUB NUMSUB', *args)

    def cluster(self, cluster_arg, *args):
        return self.command('CLUSTER %s' % cluster_arg.upper(), *args)

    def eval(self, script, numkeys, *keys_and_args):
        """
        Execute the Lua ``script``, specifying the ``numkeys`` the script
        will touch and the key names and argument values in ``keys_and_args``.
        Returns the result of the script.
        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.command('EVAL', script, numkeys, *keys_and_args)

    def evalsha(self, sha, numkeys, *keys_and_args):
        """
        Use the ``sha`` to execute a Lua script already registered via EVAL
        or SCRIPT LOAD. Specify the ``numkeys`` the script will touch and the
        key names and argument values in ``keys_and_args``. Returns the result
        of the script.
        In practice, use the object returned by ``register_script``. This
        function exists purely for Redis API completion.
        """
        return self.command('EVALSHA', sha, numkeys, *keys_and_args)

    def script_exists(self, *args):
        """
        Check if a script exists in the script cache by specifying the SHAs of
        each script as ``args``. Returns a list of boolean values indicating if
        if each already script exists in the cache.
        """
        return self.command('SCRIPT EXISTS', *args)

    def script_flush(self):
        "Flush all scripts from the script cache"
        return self.command('SCRIPT FLUSH')

    def script_kill(self):
        "Kill the currently executing Lua script"
        return self.command('SCRIPT KILL')

    def script_load(self, script):
        "Load a Lua ``script`` into the script cache. Returns the SHA."
        return self.command('SCRIPT LOAD', script)

    def register_script(self, script):
        """
        Register a Lua ``script`` specifying the ``keys`` it will touch.
        Returns a Script object that is callable and hides the complexity of
        deal with scripts, keys, and shas. This is the preferred way to work
        with Lua scripts.
        """
        return Script(self, script)

    # GEO COMMANDS
    def geoadd(self, name, *values):
        """
        Add the specified geospatial items to the specified key identified
        by the ``name`` argument. The Geospatial items are given as ordered
        members of the ``values`` argument, each item or place is formed by
        the triad latitude, longitude and name.
        """
        if len(values) % 3 != 0:
            raise Exception("GEOADD requires places with lat, lon and name"
                            " values")
        return self.command('GEOADD', name, *values)

    def geodist(self, name, place1, place2, unit=None):
        """
        Return the distance between ``place1`` and ``place2`` members of the
        ``name`` key.
        The units must be one of the following : m, km mi, ft. By default
        meters are used.
        """
        pieces = [name, place1, place2]
        if unit and unit not in ('m', 'km', 'mi', 'ft'):
            raise Exception("GEODIST invalid unit")
        elif unit:
            pieces.append(unit)
        return self.command('GEODIST', *pieces)

    def geohash(self, name, *values):
        """
        Return the geo hash string for each item of ``values`` members of
        the specified key identified by the ``name``argument.
        """
        return self.command('GEOHASH', name, *values)

    def geopos(self, name, *values):
        """
        Return the positions of each item of ``values`` as members of
        the specified key identified by the ``name``argument. Each position
        is represented by the pairs lat and lon.
        """
        return self.command('GEOPOS', name, *values)

    def georadius(self, name, longitude, latitude, radius, unit=None,
                  withdist=False, withcoord=False, withhash=False, count=None,
                  sort=None, store=None, store_dist=None):
        """
        Return the members of the specified key identified by the
        ``name`` argument which are within the borders of the area specified
        with the ``latitude`` and ``longitude`` location and the maximum
        distance from the center specified by the ``radius`` value.
        The units must be one of the following : m, km mi, ft. By default
        ``withdist`` indicates to return the distances of each place.
        ``withcoord`` indicates to return the latitude and longitude of
        each place.
        ``withhash`` indicates to return the geohash string of each place.
        ``count`` indicates to return the number of elements up to N.
        ``sort`` indicates to return the places in a sorted way, ASC for
        nearest to fairest and DESC for fairest to nearest.
        ``store`` indicates to save the places names in a sorted set named
        with a specific key, each element of the destination sorted set is
        populated with the score got from the original geo sorted set.
        ``store_dist`` indicates to save the places names in a sorted set
        named with a specific key, instead of ``store`` the sorted set
        destination score is set with the distance.
        """
        return self._georadiusgeneric('GEORADIUS',
                                      name, longitude, latitude, radius,
                                      unit=unit, withdist=withdist,
                                      withcoord=withcoord, withhash=withhash,
                                      count=count, sort=sort, store=store,
                                      store_dist=store_dist)

    def georadiusbymember(self, name, member, radius, unit=None,
                          withdist=False, withcoord=False, withhash=False,
                          count=None, sort=None, store=None, store_dist=None):
        """
        This command is exactly like ``georadius`` with the sole difference
        that instead of taking, as the center of the area to query, a longitude
        and latitude value, it takes the name of a member already existing
        inside the geospatial index represented by the sorted set.
        """
        return self._georadiusgeneric('GEORADIUSBYMEMBER',
                                      name, member, radius, unit=unit,
                                      withdist=withdist, withcoord=withcoord,
                                      withhash=withhash, count=count,
                                      sort=sort, store=store,
                                      store_dist=store_dist)

    def _georadiusgeneric(self, command, *args, **kwargs):
        pieces = list(args)
        if kwargs['unit'] and kwargs['unit'] not in ('m', 'km', 'mi', 'ft'):
            raise Exception("GEORADIUS invalid unit")
        elif kwargs['unit']:
            pieces.append(kwargs['unit'])
        else:
            pieces.append('m', )

        for token in ('withdist', 'withcoord', 'withhash'):
            if kwargs[token]:
                pieces.append(Token(token.upper()))

        if kwargs['count']:
            pieces.extend([Token('COUNT'), kwargs['count']])

        if kwargs['sort'] and kwargs['sort'] not in ('ASC', 'DESC'):
            raise Exception("GEORADIUS invalid sort")
        elif kwargs['sort']:
            pieces.append(Token(kwargs['sort']))

        if kwargs['store'] and kwargs['store_dist']:
            raise Exception("GEORADIUS store and store_dist cant be set"
                            " together")

        if kwargs['store']:
            pieces.extend([Token('STORE'), kwargs['store']])

        if kwargs['store_dist']:
            pieces.extend([Token('STOREDIST'), kwargs['store_dist']])

        return self.command(command, *pieces, **kwargs)

    def setex(self, name, value, time):
        """
        Set the value of key ``name`` to ``value`` that expires in ``time``
        seconds. ``time`` can be represented by an integer or a Python
        timedelta object.
        """
        if isinstance(time, datetime.timedelta):
            time = time.seconds + time.days * 24 * 3600
        return self.command('SETEX', name, time, value)

    def lrem(self, name, value, num=0):
        """
        Remove the first ``num`` occurrences of elements equal to ``value``
        from the list stored at ``name``.
        The ``num`` argument influences the operation in the following ways:
            num > 0: Remove elements equal to value moving from head to tail.
            num < 0: Remove elements equal to value moving from tail to head.
            num = 0: Remove all elements equal to value.
        """
        return self.command('LREM', name, num, value)

    def zadd(self, name, *args, **kwargs):
        """
        NOTE: The order of arguments differs from that of the official ZADD
        command. For backwards compatability, this method accepts arguments
        in the form of name1, score1, name2, score2, while the official Redis
        documents expects score1, name1, score2, name2.
        If you're looking to use the standard syntax, consider using the
        StrictRedis class. See the API Reference section of the docs for more
        information.
        Set any number of element-name, score pairs to the key ``name``. Pairs
        can be specified in two ways:
        As *args, in the form of: name1, score1, name2, score2, ...
        or as **kwargs, in the form of: name1=score1, name2=score2, ...
        The following example would add four values to the 'my-key' key:
        redis.zadd('my-key', 'name1', 1.1, 'name2', 2.2, name3=3.3, name4=4.4)
        """
        pieces = []
        if args:
            if len(args) % 2 != 0:
                raise Exception("ZADD requires an equal number of "
                                "values and scores")
            pieces.extend(reversed(args))
        for pair in iteritems(kwargs):
            pieces.append(pair[1])
            pieces.append(pair[0])
        return self.command('ZADD', name, *pieces)

    def subscribed(self):
        "Indicates if there are subscriptions to any channels or patterns"
        return bool(self.channels or self.patterns)

    def punsubscribe(self, *args):
        """
        Unsubscribe from the supplied patterns. If empy, unsubscribe from
        all patterns.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        return self.command('PUNSUBSCRIBE', *args)

    def subscribe(self, *args, **kwargs):
        """
        Subscribe to channels. Channels supplied as keyword arguments expect
        a channel name as the key and a callable as the value. A channel's
        callable will be invoked automatically when a message is received on
        that channel rather than producing a message via ``listen()`` or
        ``get_message()``.
        """
        if args:
            args = list_or_args(args[0], args[1:])
        new_channels = {}
        new_channels.update(dict.fromkeys(imap(self.encode, args)))
        for channel, handler in iteritems(kwargs):
            new_channels[self.encode(channel)] = handler
        ret_val = self.command('SUBSCRIBE', *iterkeys(new_channels))
        # update the channels dict AFTER we send the command. we don't want to
        # subscribe twice to these channels, once for the command and again
        # for the reconnection.
        self.channels.update(new_channels)
        return ret_val

    def encode(self, value):
        """
        Encode the value so that it's identical to what we'll
        read off the connection
        """
        if self.decode_responses and isinstance(value, bytes):
            value = value.decode(self.encoding, self.encoding_errors)
        elif not self.decode_responses and isinstance(value, unicode):
            value = value.encode(self.encoding, self.encoding_errors)
        return value

    def unsubscribe(self, *args):
        return self.command('UNSUBSCRIBE', *args)


class BasePipeline(object):
    """
    Pipelines provide a way to transmit multiple commands to the Redis server
    in one transmission.  This is convenient for batch processing, such as
    saving all the values in a list to Redis.
    All commands executed within a pipeline are wrapped with MULTI and EXEC
    calls. This guarantees all commands executed in the pipeline will be
    executed atomically.
    Any command raising an exception does *not* halt the execution of
    subsequent commands in the pipeline. Instead, the exception is caught
    and its instance is placed into the response list returned by execute().
    Code iterating over the response list should be able to deal with an
    instance of an exception as a potential value. In general, these will be
    ResponseError exceptions, such as those raised when issuing a command
    on a key of a different datatype.
    """

    UNWATCH_COMMANDS = set(('DISCARD', 'EXEC', 'UNWATCH'))

    def __init__(self, connection_pool, response_callbacks, transaction,
                 shard_hint):
        self.connection_pool = connection_pool
        self.connection = None
        self.response_callbacks = response_callbacks
        self.transaction = transaction
        self.shard_hint = shard_hint

        self.watching = False
        self.reset()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __del__(self):
        try:
            self.reset()
        except Exception:
            pass

    def __len__(self):
        return len(self.command_stack)

    def reset(self):
        self.command_stack = []
        self.scripts = set()
        # make sure to reset the connection state in the event that we were
        # watching something
        if self.watching and self.connection:
            try:
                # call this manually since our unwatch or
                # immediate_execute_command methods can call reset()
                self.connection.send_command('UNWATCH')
                self.connection.read_response()
            except Exception:
                # disconnect will also remove any previous WATCHes
                self.connection.disconnect()
        # clean up the other instance attributes
        self.watching = False
        self.explicit_transaction = False
        # we can safely return the connection to the pool here since we're
        # sure we're no longer WATCHing anything
        if self.connection:
            self.connection_pool.release(self.connection)
            self.connection = None

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            raise Exception('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise Exception('Commands without an initial WATCH have already '
                            'been issued')
        self.explicit_transaction = True

    def execute_command(self, *args, **kwargs):
        if (self.watching or args[0] == 'WATCH') and \
                not self.explicit_transaction:
            return self.immediate_execute_command(*args, **kwargs)
        return self.pipeline_execute_command(*args, **kwargs)

    def immediate_execute_command(self, *args, **options):
        """
        Execute a command immediately, but don't auto-retry on a
        ConnectionError if we're already WATCHing a variable. Used when
        issuing WATCH or subsequent commands retrieving their values but before
        MULTI is called.
        """
        command_name = args[0]
        conn = self.connection
        # if this is the first call, we need a connection
        if not conn:
            conn = self.connection_pool.get_connection(command_name,
                                                       self.shard_hint)
            self.connection = conn

    def pipeline_execute_command(self, *args, **options):
        """
        Stage a command to be executed when execute() is next called
        Returns the current Pipeline object back so commands can be
        chained together, such as:
        pipe = pipe.set('foo', 'bar').incr('baz').decr('bang')
        At some other point, you can then run: pipe.execute(),
        which will execute all commands queued in the pipe.
        """
        self.command_stack.append((args, options))
        return self

    def load_scripts(self):
        # make sure all scripts that are about to be run on this pipeline exist
        scripts = list(self.scripts)
        immediate = self.immediate_execute_command
        shas = [s.sha for s in scripts]
        # we can't use the normal script_* methods because they would just
        # get buffered in the pipeline.
        exists = immediate('SCRIPT', 'EXISTS', *shas, **{'parse': 'EXISTS'})
        if not all(exists):
            for s, exist in izip(scripts, exists):
                if not exist:
                    s.sha = immediate('SCRIPT', 'LOAD', s.script,
                                      **{'parse': 'LOAD'})

    def execute(self, raise_on_error=True):
        "Execute all the commands in the current pipeline"
        stack = self.command_stack
        if not stack:
            return []
        if self.scripts:
            self.load_scripts()
        if self.transaction or self.explicit_transaction:
            execute = self._execute_transaction
        else:
            execute = self._execute_pipeline

        conn = self.connection
        if not conn:
            conn = self.connection_pool.get_connection('MULTI',
                                                       self.shard_hint)
            # assign to self.connection so reset() releases the connection
            # back to the pool after we're done
            self.connection = conn

        try:
            return execute(conn, stack, raise_on_error)
        except Exception as e:
            conn.disconnect()
            if not conn.retry_on_timeout and isinstance(e, TimeoutError):
                raise
            # if we were watching a variable, the watch is no longer valid
            # since this connection has died. raise a Exception, which
            # indicates the user should retry his transaction. If this is more
            # than a temporary failure, the WATCH that the user next issues
            # will fail, propegating the real ConnectionError
            if self.watching:
                raise Exception("A ConnectionError occured on while watching "
                                "one or more keys")
            # otherwise, it's safe to retry since the transaction isn't
            # predicated on any state
            return execute(conn, stack, raise_on_error)
        finally:
            self.reset()

    def watch(self, *names):
        "Watches the values at keys ``names``"
        if self.explicit_transaction:
            raise Exception('Cannot issue a WATCH after a MULTI')
        return self.execute_command('WATCH', *names)

    def unwatch(self):
        "Unwatches all previously specified keys"
        return self.watching and self.execute_command('UNWATCH') or True

    def script_load_for_pipeline(self, script):
        "Make sure scripts are loaded prior to pipeline execution"
        # we need the sha now so that Script.__call__ can use it to run
        # evalsha.
        if not script.sha:
            script.sha = self.immediate_execute_command('SCRIPT', 'LOAD',
                                                        script.script,
                                                        **{'parse': 'LOAD'})
        self.scripts.add(script)


class Script(object):
    "An executable Lua script object returned by ``register_script``"

    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = ''

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        # make sure the Redis server knows about the script
        if isinstance(client, BasePipeline):
            # make sure this script is good to go on pipeline
            client.script_load_for_pipeline(self)
        try:
            return client.evalsha(self.sha, len(keys), *args)
        except Exception:
            # Maybe the client is pointed to a differnet server than the client
            # that created this instance?
            self.sha = client.script_load(self.script)
            return client.evalsha(self.sha, len(keys), *args)


if __name__ == '__main__':
    rlite = RliteDB("kkkkk.test")

    print rlite.hmset('key', {"d": 3, "fff": 5})

    print rlite.hmset("mhash", {"a": 1, "b": 2})
    print rlite.hmget("mhash", "a", "b")

    print rlite.get('key')

    print rlite.rpush('mylist', '1', '2', '3')
    print rlite.lrange('mylist', '0', '-1')
    print rlite.exists("mhash")
