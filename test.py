import ctypes
import threading
import time


def terminate_thread(thread):
    """Terminates a python thread from another thread.

    :param thread: a threading.Thread instance
    """
    if not thread.isAlive():
        return

    exc = ctypes.py_object(SystemExit)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread.ident), exc)
    print(res)
    if res == 0:
        raise ValueError("nonexistent thread id")
    elif res > 1:
        # """if it returns a number greater than one, you're in trouble,
        # and you should call it again with exc=NULL to revert the effect"""
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


def sleep():
    time.sleep(10)


def wrap(fn):
    t = threading.Thread(target=fn, daemon=True)
    t.start()
    t.join(1)

t = threading.Thread(target=sleep)
t.start()
print(time.time())
time.sleep(1)
print(time.time())
#terminate_thread(t)
try:
    t._stop()
except Exception as e:
    pass
t.join()
print(time.time())