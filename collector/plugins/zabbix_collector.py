import logging
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor
from beaker.cache import CacheManager
from ..collectors import BaseCollector
from ..metric import Metric

cache = CacheManager(type='memory')


class AutoDelete:
    def __init__(self, autodelete_api, autodelete_token, **kwargs):
        self.url = autodelete_api
        self.token = autodelete_token

    def run(self, host):
        logging.info("delete files for {}".format(host))
        res = requests.get(self.url, params={'ip': host}, headers={'Authorization': 'token {}'.format(self.token)})
        logging.info("request delete files response {}".format(res.text))


class CMDBClient:
    def __init__(self, cmdb_url, cmdb_username, cmdb_password, default_recipients, **kwargs):
        logging.error(cmdb_url)
        self.url = cmdb_url
        self.username = cmdb_username
        self.password = cmdb_password
        self.default_recipients = default_recipients.values()

    @property
    @cache.cache('cmdb_token', expire=3600)
    def token(self):
        res = requests.post('{}/api/cmdb/token/'.format(self.url),
                            json={'username': self.username, 'password': self.password})
        return {'Authorization': 'token {}'.format(res.json().get('token'))}

    @cache.cache('user_info', expire=86400)
    def get_user_info(self, name):
        res = requests.get('{}/api/cmdb/contacts/contact.json'.format(self.url),
                           params={'search': name}, headers=self.token)
        for user in res.json().get('results'):
            return user.get('mobile')

    @cache.cache('get_recipients_by_host', expire=3600)
    def get_recipients_by_host(self, host):
        users = set()
        res = requests.get('{}/api/cmdb/applications/applicationgroup.json'.format(self.url),
                           params={'ipaddresses': host}, headers=self.token)
        for app in res.json().get('results'):
            for field in ('owner', 'backup_owner', 'ops_owner'):
                user = app.get(field)
                if user is not None:
                    users.add(user)
        return (self.get_user_info(name) for name in users)

    @cache.cache('get_apps_by_host', expire=3600)
    def get_apps_by_host(self, host):
        res = requests.get('{}/api/cmdb/applications/applicationgroup.json'.format(self.url),
                           params={'ipaddresses': host}, headers=self.token)
        apps = set()
        for app in res.json().get('results'):
            if app:
                apps.add(app.get('application'))
        return apps

    @cache.cache('get_recipients_by_app', expire=3600)
    def get_recipients_by_app(self, app):
        users = set()
        res = requests.get('{}/api/cmdb/applications/application.json'.format(self.url),
                           params={'name': app}, headers=self.token)
        for app in res.json().get('results'):
            for field in ('owner', 'backup_owner', 'ops_owner'):
                user = app.get(field)
                if user is not None:
                    users.add(user)
        return (self.get_user_info(name) for name in users)

    def get_recipients(self, host):
        yield from self.default_recipients
        yield from self.get_recipients_by_host(host)
        for app in self.get_apps_by_host(host):
            yield from self.get_recipients_by_app(app)

    def recipients(self, host):
        return set(self.get_recipients(host))


def send(url, message, recipients):
    for recipient in recipients:
        try:
            requests.post(url, params={'content': message.encode('utf-8'),
                                       'appId': 'sendplatform.ops.ymatou.cn',
                                       'phone': recipient}, timeout=10)
            logging.warning('sent message {} to {}'.format(message, recipient))
        except Exception as e:
            logging.error('sent message {} to {} fail: {}'.format(message, recipient, e))


def render(template: str, data):
    template = template.replace('{ITEM.VALUE}', data['items'][0]['lastvalue'])
    template = template.replace('{HOST.NAME}', data['hosts'][0]['name'])
    return template


priority_map = {
    0: 'not classified',
    1: 'information',
    2: 'warning',
    3: 'average',
    4: 'high',
    5: 'disaster'
}


class ZabbixCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)
        self.url = context.params.get('zabbix_url')
        self.executor = ThreadPoolExecutor(max_workers=20)
        self.cmdb_client = CMDBClient(**context.params)
        self.auto_delete = AutoDelete(**context.params)

    @property
    def token(self):
        payload = {
            "jsonrpc": "2.0",
            "params": {
                "user": self.context.params.get('zabbix_username'),
                "password": self.context.params.get('zabbix_password')
            },
            "method": "user.login",
            "auth": None,
            "id": 0
        }
        res = requests.post(self.url, json=payload)
        return res.json().get("result")

    def get_event(self):
        now = datetime.datetime.now()
        last = self.context.shard_data.get('{}::last'.format(self.context.name),
                                           now - datetime.timedelta(seconds=self.context.interval))
        self.context.shard_data['{}::last'.format(self.context.name)] = now
        payload = {
            "jsonrpc": "2.0",
            "method": "trigger.get",
            "params": {
                "only_true": 1,
                "skipdependent": 1,
                "monitored": 1,
                "active": 1,
                "output": ["priority", "description"],
                "selectHosts": ["name", "host"],
                "selectItems": ["name", "lastvalue"],
                "selectLastEvent": ["ns", "clock"],
                "limit": 100,
                "sortfield": "lastchange",
                "lastChangeSince": int(last.timestamp()),
                "withLastEventUnacknowledged": 1,
                "filter": {
                    "value": 1
                },
                "sortorder": "DESC"
            },
            "auth": self.token,
            "id": 1
        }
        res = requests.post(self.url, json=payload)
        metrics = []
        for event in res.json().get('result', []):
            hosts = [x['host'] for x in event['hosts']]
            apps = set()
            recipients = set()
            for host in hosts:
                apps.update(self.cmdb_client.get_apps_by_host(host))
                recipients.update(self.cmdb_client.recipients(host))
            items = event['items']
            priority = int(event['priority'])
            tags = {
                'id': event['triggerid'],
                'message': render(event['description'], event),
                'source': 'zabbix',
                'level': priority_map.get(priority)
            }
            if len(hosts) > 0:
                tags['host'] = ','.join(hosts)
            if len(apps) > 0:
                tags['app'] = ','.join(apps)
            if len(items) > 0:
                tags['item'] = items[0]['name']
            last_event = '{}{}'.format(event['lastEvent']['clock'], event['lastEvent']['ns'])
            timestamp = datetime.datetime.fromtimestamp(int(last_event) / 1000000000)
            metrics.append(Metric('zabbix_trigger', priority, tags=tags, timestamp=timestamp))
            if priority in (2, 5):
                message = '【zabbix】【{level}】【{0}】{message} host: {host} apps: {app}'.format(
                    timestamp.strftime('%Y-%m-%d %H:%M:%S'), **tags)
                self.executor.submit(send, self.context.params.get('sms_url'), message, recipients)
            if tags['message'].startswith('Free disk space is less than'):
                for host in hosts:
                    self.executor.submit(self.auto_delete.run, host)
        return metrics

    def run(self):
        return self.get_event()
