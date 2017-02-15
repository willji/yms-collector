import datetime
import logging
import requests
from ..collectors import BaseCollector
from ..metric import Metric


class SellerOnlineStatusCollector(BaseCollector):
    @staticmethod
    def to_metric(r):
        dt = datetime.datetime.now()
        status = r['Status']
        count = r['StatusCount']
        return Metric('seller_online_status', count, dt, tags={'status': status}, db="yms", prefix="biz")

    def run(self):
        params = {'StarTime': '2016-01-01 00:00:00', 'EndTime': "2076-12-31 23:59:59"}
        try:
            req = requests.get(self.context.params['url'], params=params)
            resp = req.json()
            ms = [self.to_metric(x) for x in resp['Result']]
            return ms
        except Exception as e:
            logging.error(e)
        return []
