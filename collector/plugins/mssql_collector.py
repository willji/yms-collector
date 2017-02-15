import datetime
import pymssql
import logging
from ..collectors import BaseCollector
from ..metric import Metric


class MssqlCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)

    def make_metric(self, data, mtype, host):
            tags = {}
            key = self.context.params.get('key')
            tags['host'] = host
            tags['type'] = mtype
            value = data[1]
            timestamp = datetime.datetime.now()
            return Metric(key, value, tags=tags, timestamp=timestamp)

    def run_with_mysql(self):
        metrics = []
        conn = None
        cur = None
        try:
            hosts = self.context.params.get('host')
            for host in hosts:
                conn = pymssql.connect(server=host,
                                       user=self.context.params.get('user', 'root'),
                                       password=self.context.params.get('password'),
                                       database=self.context.params.get('db'))
                sqls = self.context.params.get("sql")
                for mtype,sql in sqls.items():
                    cur = conn.cursor()
                    if isinstance(sql, list):
                        sql = sql[0]
                    cur.execute(sql)
                    datas=cur.fetchall()
                    for data in datas:
                        metrics.append(self.make_metric(data, mtype, host))
                    cur.close()
                conn.close()
        except Exception as e:
            logging.error("run mssql collector error: {}".format(e))
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            return metrics

    def run(self):
        return self.run_with_mysql()
