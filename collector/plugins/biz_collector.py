import logging
import datetime
import pymysql
from ..collectors import BaseCollector
from ..metric import Metric


class BusinessCollector(BaseCollector):
    QUERY = r'''SELECT num, begintime FROM `{table}` WHERE `source`='{source}' AND begintime > '{last}' ORDER BY begintime DESC'''

    def __init__(self, context):
        super().__init__(context)
        self.last = datetime.datetime.now() - datetime.timedelta(seconds=self.context.interval)

    def connect(self):
        conn = pymysql.connect(host=self.context.params.get('host', 'localhost'),
                               port=self.context.params.get('port', 3306),
                               user=self.context.params.get('user', 'root'),
                               password=self.context.params.get('password'),
                               charset=self.context.params.get('charset', 'utf8'),
                               db=self.context.params.get('db'))
        return conn

    @staticmethod
    def get_tables(conn):
        with conn.cursor() as cursor:
            cursor.execute(r'''SELECT tbname, `source` FROM source_info''')
            yield from cursor.fetchall()

    def run(self):
        metrics = []
        now = datetime.datetime.now()
        logging.debug("now is {}".format(now))
        logging.debug("last check time is {}".format(self.last))
        conn = self.connect()
        with conn as cursor:
            with cursor:
                for table, source in self.get_tables(conn):
                    query = self.QUERY.format(table=table, source=source, last=self.last.strftime('%Y-%m-%d %H:%M:%S'))
                    ret = cursor.execute(query)
                    if ret == 0:
                        query = self.QUERY.format(table=table, source=source,
                                                  last=(now-datetime.timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:%S'))
                        ret = cursor.execute(query)
                    logging.debug("execute {} return {} rows".format(query, ret))
                    for row in cursor.fetchall():
                        tags = {'source': source}
                        metrics.append(Metric(table, row[0], tags=tags, timestamp=row[1]))
        self.last = now
        conn.close()
        return metrics
