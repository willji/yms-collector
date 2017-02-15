import re
import time
from pymongo import MongoClient
from ..collectors import BaseCollector
from ..metric import Metric


class AppCollector(BaseCollector):
    def check(self, counter):
        if re.match('^/hb/.*$', counter):
            return None
        elif re.match('^/n[0-9]*$', counter):
            return '/n'
        elif re.match('^/page/index/.*$', counter):
            return '/page/index'
        elif re.match('^/ProductsManage/delete/p_.*$', counter):
            return '/ProductsManage/delete'
        else:
            return counter

    def run(self):
        metrics = []
        client = MongoClient("mongodb://{}:{}".format(self.context.params.get('host', 'localhost'), self.context.params.get('port', 6379)))
        db = client.performance
        table = 'perfdata{0}'.format(time.strftime("%y%m%d", time.localtime(time.time())))
        data = db[table].find({'Time':{'$gte':time.strftime("%H%M", time.localtime(time.time())),\
                                        '$lte':time.strftime("%H%M", time.localtime(time.time()))}})
        for document in data:
            app_info = list(db.PerfCounter.find({'_id': document.get('IndexId')})) 
            if app_info:
                appid = app_info[0].get('AppId')
                counter = app_info[0].get('Counter')
                counter = self.check(counter)
                if not counter:
                    continue
                pv = 0
                long_time = 0
                consuming_time = {}
                for j in document.get('UrlCounters'):
                    consuming_time[j['K']] = j['V']
                    pv += j['V']
                    if int(j['K'][2:]) > 500:
                        long_time += j['V'] 
                consuming_time['pv'] = pv
                tags = {'app': appid, 'counter':counter}
                metrics.append(Metric('time', consuming_time, tags=tags))
                metrics.append(Metric('long_time', long_time, tags=tags))
        return metrics
