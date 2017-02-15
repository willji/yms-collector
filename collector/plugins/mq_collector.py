import requests
from ..collectors import BaseCollector
from ..metric import Metric


class MQCollector(BaseCollector):
    def run(self):
        metrics = []
        url = self.context.params.get('host', 'localhost')
        r = requests.get(url, auth=(self.context.params.get('user', 'guest'), self.context.params.get('password', 'guest')))
        data = r.json()
        data.sort(key=lambda x:x['messages_ready'], reverse=True)
        for i in data:
            name = i.get('name')
            tags = {'app': name}   
            messages = i.get('messages')
            consumers = i.get('consumers')
            messages_ready = i.get('messages_ready')
            unack = i.get('messages_unacknowledged_details').get('rate')
            metrics.append(Metric('messages', messages, tags=tags)) 
            metrics.append(Metric('consumers', consumers, tags=tags)) 
            metrics.append(Metric('messages_ready', messages_ready, tags=tags)) 
            metrics.append(Metric('unack', unack, tags=tags)) 
            if i.get('message_stats'):
                publish = i.get('message_stats').get('publish_details').get('rate')
                deliver_get = i.get('message_stats').get('deliver_get_details').get('rate')
                ack = i.get('message_stats').get('ack_details').get('rate')
                metrics.append(Metric('publish', publish, tags=tags)) 
                metrics.append(Metric('deliver_get', deliver_get, tags=tags)) 
                metrics.append(Metric('ack', ack, tags=tags)) 
        return metrics
