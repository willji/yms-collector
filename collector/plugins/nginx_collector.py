import requests
import datetime
from ..collectors import BaseCollector
from ..metric import Metric


class NginxCollector(BaseCollector):
    def get_nginx(self):
        r = requests.post(self.context.params.get('token_url', ''), data={'username':self.context.params.get('username', 'opsadmin'), 'password':self.context.params.get('password', '')}) 
        headers= {'Authorization':'token {0}'.format(r.json()['token'])}
        r = requests.get(self.context.params.get('data_url', ''), headers=headers)
        data = []
        for i in r.json():
            if i.get('parent') == 2:
                name = i.get('name')
                url = '{}?format=json'.format(i.get('link_address'))
                data.append([name, url])
        return data

    def run(self):
        timestamp = datetime.datetime.now()
        nginx = self.get_nginx()
        metrics = []
        for i in nginx:
            try:
                r = requests.get(i[1], timeout=0.2)
                data = r.json().get('servers').get('server')
                if data:
                    for j in data:
                        nginx_name = i[0]
                        name = j.get('name') 
                        upstream = j.get('upstream')
                        value = j.get('status')
                        tags = {'nginx_name': nginx_name, 'name': name, 'upstream': upstream}
                        metrics.append(Metric('status', value, tags=tags, timestamp=timestamp))
            except:
                continue
        return metrics
