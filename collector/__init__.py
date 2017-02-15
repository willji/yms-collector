import threading
import logging
import importlib
import datetime
import queue
import json
import os
from .metric import Metric


class Context:
    shard_data = {}

    def __init__(self, name, interval=0, timeout=0, params=None):
        self.name = name
        self.interval = interval
        self.timeout = timeout
        self.params = params
        if params is None:
            self.params = {}

    def __getattr__(self, item):
        return None


class Collector:
    def __init__(self, name, prefix, interval, timeout, mod):
        self.name = name
        self.prefix = prefix
        self.interval = interval
        self.timeout = timeout
        self.mod = mod
        self.next_execute_time = datetime.datetime.now() + datetime.timedelta(seconds=interval)


class Backend:
    def __init__(self, name, mod, interval=3, item_length=128, queue_length=1024):
        self.name = name
        self.interval = interval
        self.item_length = item_length
        self.mod = mod
        self.queue = queue.Queue(maxsize=queue_length)
        self.__event = threading.Event()

    def put(self, metric):
        try:
            self.queue.put_nowait(metric)
        except queue.Full:
            logging.warning('backend {} queue full'.format(self.name))

    def send(self):
        metrics = []
        start = datetime.datetime.now()
        while not self.__event.is_set():
            try:
                current = datetime.datetime.now()
                if (current - start).total_seconds() >= self.interval or len(metrics) >= self.item_length:
                    start = current
                    self.mod.send(metrics)
                    metrics = []
                metric = self.queue.get(timeout=0.1)
                metrics.append(metric)
            except queue.Empty:
                pass
            except Exception as e:
                logging.error('backend {} send metrics error: {}'.format(self.name, e))

    def shutdown(self):
        self.__event.set()


class CollectorManager:
    def __init__(self, config, home):
        self.home = home
        self.config = config
        self.collectors = {}
        self.queue = queue.Queue()
        self._event = threading.Event()
        self.backends = {}
        self.load_config()

    def load_config_from_file(self, file):
        try:
            with open(file) as f:
                config = json.loads(f.read())
                if not self.config.get('backend'):
                    self.config['backend'] = []
                self.config['backend'].extend(config.get('backend', []))
                if not self.config.get('collector'):
                    self.config['collector'] = []
                self.config['collector'].extend(config.get('collector', []))
        except Exception as e:
            logging.error('load config from {} error: {}'.format(file, e))

    def load_config(self):
        if self.config.get('include'):
            for root, _, files in os.walk(self.config.get('include').format(home=self.home)):
                for file in files:
                    self.load_config_from_file(os.path.join(root, file))

    def start(self):
        for conf in self.config.get('backend', []):
            self.load_backend(conf)
        for conf in self.config.get('collector', []):
            self.load_collector(conf)
        for backend in self.backends.values():
            t = threading.Thread(name='backend-{}'.format(backend.name), target=backend.send)
            t.daemon = True
            t.start()
        while not self._event.is_set():
            now = datetime.datetime.now()
            for name in self.collectors.keys():
                if now >= self.collectors[name].next_execute_time:
                    self.run(name)
            self._event.wait(1)

    def load_backend(self, conf):
        try:
            if conf.get('enable') is False:
                return
            name = conf['name']
            m, c = conf['plugin'].rsplit('.', 1)
            interval = conf.get('interval', 3)
            item_length = conf.get('item_length', 128)
            queue_length = conf.get('queue_length', 1024)
            context = Context(name, params=conf.get('params'))
            mod = getattr(importlib.import_module(m), c)(context)
            backend = Backend(name, mod, interval, item_length, queue_length)
            if name in self.backends.keys():
                logging.warning("backend {} is exist, skip it".format(name))
                return
            self.backends[name] = backend
        except Exception as e:
            logging.error('load backend {} error, {}'.format(conf.get('name'), e))

    def load_collector(self, conf):
        try:
            if conf.get('enable') is False:
                return
            name = conf['name']
            prefix = conf.get('prefix')
            m, c = conf['plugin'].rsplit('.', 1)
            interval = conf.get('interval', 60)
            timeout = conf.get('timeout', interval * 0.8)
            context = Context(name, interval=interval, timeout=timeout, params=conf.get('params'))
            mod = getattr(importlib.import_module(m), c)(context)
            if name in self.collectors.keys():
                logging.warning("collector {} is exist, skip it".format(name))
                return
            self.collectors[name] = Collector(name, prefix, interval, timeout, mod)
        except Exception as e:
            logging.error('load collector {} error, {}'.format(conf.get('name'), e))

    def run(self, name):
        thread = threading.Thread(name='collector-wrap-{}'.format(name), target=self.wrap_exec_collector, args=(name, ))
        thread.daemon = True
        thread.start()

    def wrap_exec_collector(self, name):
        collector = self.collectors.get(name)
        t = threading.Thread(name='collector-{}'.format(name), target=self.exec_collector, args=(collector, ))
        t.daemon = True
        t.start()
        t.join(collector.timeout)

    def exec_collector(self, collector):
        try:
            collector.next_execute_time = datetime.datetime.now() + datetime.timedelta(seconds=collector.interval)
            start = datetime.datetime.now()
            metrics = collector.mod.run()
            cost = (datetime.datetime.now() - start).total_seconds()
            if not metrics:
                metrics = []
            metrics.append(Metric('cost', cost, prefix='yms.collector', tags={'name': collector.name}))
            for m in metrics:
                if not m.prefix:
                    m.prefix = collector.prefix
                for backend in self.backends.values():
                    backend.put(m)
        except Exception as e:
            try:
                for backend in self.backends.values():
                    backend.put(Metric('error', 1, prefix='yms.collector', tags={'name': collector.name}))
            except Exception as ex:
                pass
            logging.error('execute collector {} error: {}'.format(collector.name, e))

    def shutdown(self):
        for backend in self.backends.values():
            backend.shutdown()
        self._event.set()

    def get_info(self):
        return threading.enumerate(), self.backends, self.collectors


