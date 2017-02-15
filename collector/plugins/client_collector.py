import time
import pymysql
from pykafka import KafkaClient
from ..collectors import BaseCollector
from ..metric import Metric


class ClientCollector(BaseCollector):
    def addr2dec(self, addr):
        items = [int(x) for x in addr.split(".")]
        return sum([items[i] << [24, 16, 8, 0][i] for i in range(4)])

    def get_city(self, ip):
        try:
            conn = pymysql.connect(host=self.context.params.get('db_host', 'localhost'),user=self.context.params.get('db_user', 'root'),\
                    passwd=self.context.params.get('db_password', ''),db=self.context.params.get('db_db', 'fulllink'),port=3306,charset='utf8')
            cur = conn.cursor()
            if ip:
                num = self.addr2dec(ip)
                sql = 'select `city` from ip_city where {0}>ipbeginnum order by ipbeginnum desc limit 1;'.format(num)
                cur.execute(sql)
                return cur.fetchall()[0][0]
            else:
                return '*'
        except Exception as e:
            raise e
        finally:
            cur.close()
            conn.close()

    def run(self):
        metrics = []
        client = KafkaClient(hosts="{0}:{3}, {1}:{3}, {2}:{3}".format(self.context.params.get('host1', 'localhost'), self.context.params.get('host2', 'localhost'),\
                            self.context.params.get('host3', 'localhost'), self.context.params.get('client_port', 9092)))
        topic = client.topics[b'http_request']
        balanced_consumer = topic.get_balanced_consumer(
        consumer_group=b'ymsgroup',
        auto_commit_enable=True,
        zookeeper_connect="{0}:{3}, {1}:{3}, {2}:{3}".format(self.context.params.get('host1', 'localhost'), self.context.params.get('host2', 'localhost'),\
                            self.context.params.get('host3', 'localhost'), self.context.params.get('connect_port', 2181)))
        count = 0
        begin_time = time.time()
        for message in balanced_consumer:
            if message is not None:
                if time.time()-begin_time > 50:
                    break
                else:
                    if count % 100 == 0:
                        count = 1
                        value = message.value.split(b'\001')
                        d = {}
                        for i in value:
                            if i:
                                d[i.split(b'=')[0].decode('utf8')] = i.split(b'=')[1].decode('utf8')
                        if float(d.get('time'))/1000 < time.time()+60:
                            log_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(float(d.get('time'))/1000))
                            ip = d.get('ip')
                            mobile_operator = d.get('mobile_operator', '')
                            action_param = d.get('action_param')
                            code = action_param.split(';')[0].split(':')[1]
                            resp_time = float(action_param.split(';')[1].split(':')[1])
                            nslookup = float(d.get('time'))/10000000
                            target = d.get('target').split('?')[0]
                            city = self.get_city(ip)
                            tags = {'app': target}
                            metrics.append(Metric('log_time', log_time, tags=tags))
                            metrics.append(Metric('ip', ip, tags=tags))
                            metrics.append(Metric('city', city, tags=tags))
                            metrics.append(Metric('mobile_operator', mobile_operator, tags=tags))
                            metrics.append(Metric('code', code, tags=tags))
                            metrics.append(Metric('resp_time', resp_time, tags=tags))
                            metrics.append(Metric('nslookup', nslookup, tags=tags))
                    else:
                        count += 1
                        continue
        return metrics

