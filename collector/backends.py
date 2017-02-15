import requests
import logging
from itertools import groupby


class BaseBackend:
    def __init__(self, context):
        self.context = context

    def send(self, metrics):
        return NotImplementedError()


class InfluxDBBackend(BaseBackend):
    def __init__(self, context):
        super().__init__(context)
        if context.params.get('type') == 'influxdb':
            self.base_url = 'http://{}:{}/write'.format(context.params.get('host', 'localhost'),
                                                        context.params.get('port', 8086))
        else:
            self.base_url = 'http://{}:{}/kapacitor/v1/write'.format(context.params.get('host', 'localhost'),
                                                                     context.params.get('port', 9092))

    def send(self, metrics):
        for db, line in self.build_line_protocol(metrics):
            logging.debug(line)
            try:
                params = {'db': db, 'rp': self.context.params.get('rp', 'default')}
                resp = requests.post(self.base_url, data=line.encode('utf-8'), params=params)
                logging.debug(resp.url)
                if resp.status_code >= 300:
                    logging.error("send metrics to influxdb error: {} lines: {}".format(resp.content, line))
            except Exception as e:
                logging.error("send metrics to influxdb error: {} lines: {}".format(e, line))

    @staticmethod
    def escape(src, special):
        dst = []
        for i, c in enumerate(str(src)):
            if i > 0 and dst[i-1] == '\\':
                dst[i-1] = '\\{}'.format(c)
            elif c in special:
                dst.append('\\{}'.format(c))
            else:
                dst.append(c)
        return ''.join(dst)

    @staticmethod
    def warp(v):
        if isinstance(v, str):
            return '"{}"'.format(v)
        return v

    def build_line_protocol(self, metrics):
        for db, ms in groupby(metrics, key=lambda x: x.db):
            lines = []
            for m in ms:
                if isinstance(m.value, dict):
                    value = ','.join(['{}={}'.format(k, self.warp(v)) for k, v in m.value.items()])
                else:
                    value = 'value={}'.format(self.warp(m.value))
                measurement = '{}.{}'.format(m.prefix, m.key) if m.prefix else m.key
                tags = ','.join(['{}={}'.format(k, InfluxDBBackend.escape(v, ' \\=,')) for k, v in m.tags.items()])
                lines.append('{},{} {} {}'.format(measurement, tags, value, m.timestamp))
            yield db if db is not None else self.context.params.get('default_db', 'yms'), '\n'.join(lines)
