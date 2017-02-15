import datetime


class Metric:
    def __init__(self, key, value, timestamp=None, tags=None, db=None, prefix=None):
        self.db = db
        self.key = key
        self.value = value
        self._timestamp = timestamp if timestamp is not None else datetime.datetime.now()
        self.tags = tags if tags is not None else {}
        self.prefix = prefix

    def add_tag(self, key, value):
        self.tags[key] = value

    @property
    def timestamp(self):
        return int(self._timestamp.timestamp() * 1000 * 1000000)
