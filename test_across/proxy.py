import unittest
import across
import copy
import weakref
import gc
import threading
from .utils import make_connection, Box


class Counter(object):
    def __init__(self, value=0):
        self.__value = value

    def get(self):
        return self.__value

    def add(self, value):
        self.__value += value


def get_args(*args, **kwargs):
    return args, kwargs


def call(func, *args, **kwargs):
    return func(*args, **kwargs)


class Add(object):
    def __call__(self, left, right):
        return left + right


class ProxyTestCase(unittest.TestCase):
    def test_create_without_args(self):
        with make_connection() as conn:
            proxy = conn.create(Counter)
            self.assertIsInstance(proxy, across.Proxy)

    def test_deepcopy(self):
        with make_connection() as conn:
            proxy = conn.create(Counter, 42)
            obj = copy.deepcopy(proxy)
            self.assertNotIsInstance(obj, across.Proxy)
            self.assertIsInstance(obj, Counter)
            self.assertEqual(obj.get(), 42)
            obj.add(1)
            self.assertEqual(obj.get(), 43)
            self.assertEqual(proxy.get(), 42)

    def test_create_with_args(self):
        with make_connection() as conn:
            self.assertEqual(conn.create(Counter, 1).get(), 1)
            self.assertEqual(conn.create(Counter, value=1).get(), 1)

    def test_create_special_name_handling(self):
        with make_connection() as conn:
            proxy = conn.create(get_args, self=1)
            self.assertEqual(copy.deepcopy(proxy), ((), {'self': 1}))
            proxy = conn.create(get_args, func=1)
            self.assertEqual(copy.deepcopy(proxy), ((), {'func': 1}))
            with self.assertRaises(TypeError):
                conn.create()
            with self.assertRaises(TypeError):
                across.Connection.create()

    def test_replicate(self):
        with make_connection() as conn:
            orig = Counter()
            proxy = conn.replicate(orig)
            self.assertIsInstance(proxy, across.Proxy)
            self.assertEqual(proxy.get(), 0)
            orig.add(1)
            self.assertEqual(proxy.get(), 0)

    def test_local(self):
        with make_connection() as conn:
            proxy = conn.call(Box(lambda: across.Local(5)))
            self.assertIsInstance(proxy, across.Proxy)
            self.assertEqual(copy.deepcopy(proxy), 5)

    def test_local_with_lambda(self):
        with make_connection() as conn:
            self.assertEqual(conn.call(call, across.Local(lambda: 5)), 5)

    def test_local_with_func(self):
        with make_connection() as conn:
            self.assertEqual(conn.call(call, across.Local(get_args), 5), ((5, ), {}))

    def test_local_with_builtin_func(self):
        with make_connection() as conn:
            self.assertEqual(conn.call(call, across.Local(max), 5, 10), 10)


class ProxyDelTestCase(unittest.TestCase):
    def test_del(self):
        with make_connection() as conn:
            obj_weakref = Box()

            def create_remotely():
                obj = Counter()
                obj_weakref.value = weakref.ref(obj)
                return across.Local(obj)
            proxy = conn.call(Box(create_remotely))
            self.assertIsInstance(proxy, across.Proxy)
            self.assertIsNotNone(obj_weakref())

            proxy = None
            self.__collect(conn)
            self.assertIsNone(obj_weakref())

    def __collect(self, conn):
        gc.collect()
        # wait for utility thread to process all tasks
        event = threading.Event()
        across._call_elsewhere(event.set)
        event.wait()


class Empty(object):
    pass


class AutoProxyTestCase(unittest.TestCase):
    def test_empty_class(self):
        with make_connection() as conn:
            proxy = conn.create(Empty)
            obj = copy.deepcopy(proxy)
            self.assertIsInstance(obj, Empty)

    def test_nonexistent_attribute(self):
        with make_connection() as conn:
            proxy = conn.create(Empty)
            with self.assertRaises(AttributeError):
                proxy.missing

    def test_list(self):
        with make_connection() as conn:
            proxy = conn.create(list)
            proxy.append(1)
            self.assertEqual(copy.deepcopy(proxy), [1])
            proxy.extend([2, 3])
            self.assertEqual(copy.deepcopy(proxy), [1, 2, 3])
            self.assertTrue(1 in proxy)
            self.assertFalse(4 in proxy)
            self.assertEqual(len(proxy), 3)

    def test_cyclic_list(self):
        with make_connection() as conn:
            proxy = conn.create(list)
            proxy.append(proxy)
            obj = copy.deepcopy(proxy)
            self.assertEqual(len(obj), 1)
            self.assertIs(obj, obj[0])

    def test_dict(self):
        with make_connection() as conn:
            proxy = conn.create(dict)
            with self.assertRaises(KeyError):
                proxy[1]
            proxy[1] = -1
            self.assertEqual(proxy[1], -1)
            self.assertEqual(proxy.get(1), -1)
            self.assertTrue(1 in proxy)
            self.assertFalse(2 in proxy)
            del proxy[1]
            with self.assertRaises(KeyError):
                del proxy[1]

    def test_callable(self):
        with make_connection() as conn:
            proxy = conn.replicate(Add())
            self.assertEqual(proxy(1, 2), 3)
