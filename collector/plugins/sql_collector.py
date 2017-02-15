import string
import datetime
import pymysql
import logging
from pymysql.cursors import DictCursor
from ..collectors import BaseCollector
from ..metric import Metric


class QueryFormatter(string.Formatter):
    def __init__(self):
        self.conversion = None

    def convert_field(self, value, conversion):
        if conversion == 'T':
            self.conversion = 'T'
            return super().convert_field(value, None)
        return super().convert_field(value, conversion)

    def format_field(self, value, format_spec):
        if self.conversion == 'T':
            return super().format(value.strftime(format_spec), '')
        return super().format_field(value, '')


class SQLCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)
        self.now = datetime.datetime.now()
        self.last = self.now - datetime.timedelta(seconds=self.context.interval)

    def timestamp(self, row, prop):
        field = prop.get('field', 'timestamp')
        value = row.get(field)
        if isinstance(value, datetime.datetime):
            return value
        if value is None:
            return self.now
        if isinstance(value, (int, float)):
            if 10**9 < value < 10**10:
                return datetime.datetime.fromtimestamp(value)
            if value >= 10**10:
                length = len(str(int(value)))
                return datetime.datetime.fromtimestamp(value / (10**(length-10)))
            return self.now
        if isinstance(value, str):
            fmt = prop.get('fmt', '%Y-%m-%d %H:%M:%S')
            return datetime.datetime.strptime(value, fmt)
        return self.now

    def make_metric(self, row):
        for key, prop in self.context.params.get('mapping'):
            if prop.get('template'):
                key = key.format(**row)
            tags = {}
            for k, c in prop.get('tags', {}).items():
                tags[key] = row.get(c)
            value = row.get(prop.get('value', 'value'))
            try:
                timestamp = self.timestamp(row, prop.get("timestamp", {}))
            except Exception as e:
                logging.warning("get timestamp fail {}".format(e))
                timestamp = self.now
            return Metric(key, value, tags=tags, timestamp=timestamp)

    def run_with_mysql(self):
        metrics = []
        conn = None
        try:
            conn = pymysql.connect(host=self.context.params.get('host', 'localhost'),
                                   port=self.context.params.get('port', 3306),
                                   user=self.context.params.get('user', 'root'),
                                   password=self.context.params.get('password'),
                                   db=self.context.params.get('db'))
            with conn.cursor(cursor=DictCursor) as cursor:
                formatter = QueryFormatter()
                query = formatter.format(self.context.params.get('query'), now=self.now, last=self.last)
                print(query)
                cursor.execute(query)
                for row in cursor.fetchall():
                    metrics.append(self.make_metric(row))
        except Exception as e:
            logging.error("run sql collector error: {}".format(e))
        finally:
            if conn is not None:
                conn.close()
        return metrics

    def run(self):
        if self.context.params.get('driver', 'mysql') == 'mysql':
            return self.run_with_mysql()
        # TODO 实现其他数据库驱动
        return []
