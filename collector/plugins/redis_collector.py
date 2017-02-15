import redis
from ..collectors import BaseCollector
from ..metric import Metric


class RedisCollector(BaseCollector):
    def run(self):
        metrics = []
        tags = {'host': self.context.params.get('host', 'localhost'),
                'port': self.context.params.get('port', 6379)}
        instance = redis.StrictRedis(host=self.context.params.get('host', 'localhost'),
                                     port=self.context.params.get('port', 6379))
        info = instance.info()
        role = info.get('role')
        if role == 'master':
            with open('/opt/app/yms-collector/collector/config/master_items.txt') as f:
                l = f.readlines()
                l = [i.rstrip('\n') for i in l]
                for item in l:
                    if item in ['slave0', 'slave1']:
                        if info.get(item):
                            value = info.get(item).get('state')
                    else:
                        value = info.get(item)
                    metrics.append(Metric(item, value, tags=tags))
        elif role == 'slave':
            with open('/opt/app/yms-collector/collector/config/slave_items.txt') as f:
                l = f.readlines()
                l = [i.rstrip('\n') for i in l]
                for item in l:
                    value = info.get(item)
                    metrics.append(Metric(item, value, tags=tags))
        return metrics
