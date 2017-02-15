import json
import time
import requests
from influxdb import InfluxDBClient

class Mail:
    def __init__(self):
        self.client = InfluxDBClient('influxdb.ops.ymatou.cn', 80, '', '', 'yms')

class EventMail(Mail):
    def get_event(self):
        query = 'select count("message") from "yms.event" where time>now() - 1h and level=\'INFO\' or level=\'CRITICAL\' and value=\'received\' group by apps,level'
        result = self.client.query(query)
        raw_data = result.raw.get('series')
        data = []
        for i in raw_data:
            app = i.get('tags').get('apps')
            level = i.get('tags').get('level')
            count = i.get('values')[0][-1]
            data.append([app, level, count])
        data = sorted(data, key=lambda x:x[2], reverse=True)
        return data
    
    def get_html(self, data):
        msg = '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>app</td>'
        msg += '<td>level</td>'
        msg += '<td>count</td>'
        msg += '</tr>'
        for i in data:
            msg += '<tr>'
            app = i[0]
            level = i[1]
            count = i[2]
            msg += '<td>{}</td>'.format(app)
            msg += '<td>{}</td>'.format(level)
            msg += '<td>{}</td>'.format(count)
            msg += '</tr>'
        msg += '</table>'
        msg += '<br><br>'
        return msg

    def send_mail(self):
        data = self.get_event()
        html = self.get_html(data)
        url = 'http://sendplatform.ops.ymatou.cn/api/send'
        data = {'msg_title':'每小时告警汇总', 'addresses': json.dumps(['lzh@ymatou.com', 'jiwei@ymatou.com']), 'msg_type': 'mail', 'msg': html}
        r = requests.post(url, data=data)
        print(r.status_code)
        print(r.text)

class BusinessMail(Mail):
    def get_data(self):
        data = {}
        tmp_data = []
        query = 'select sum(value) from "biz.payment_sta" where time>now() - 1h group by source'
        result = self.client.query(query)
        for i in result.raw.get('series'):
            tmp_data.append([i.get('tags').get('source'), i.get('values')[0][-1]])    
        data['payment'] = tmp_data
        tmp_data = []
        query = 'select sum(value) from "biz.register_sta" where time>now() - 1h group by source'
        result = self.client.query(query)
        for i in result.raw.get('series'):
            tmp_data.append([i.get('tags').get('source'), i.get('values')[0][-1]])    
        data['register'] = tmp_data
        tmp_data = []
        query = 'select sum(value) from "biz.order_sta" where time>now() - 1h group by source'
        result = self.client.query(query)
        for i in result.raw.get('series'):
            tmp_data.append([i.get('tags').get('source'), i.get('values')[0][-1]])    
        data['order'] = tmp_data
        return data
 
    def get_html(self, data):
        msg = '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>交易来源</td>'
        msg += '<td>交易数量</td>'
        msg += '</tr>'
        for i in sorted(data.get('payment'), key=lambda x:x[-1], reverse=True):
            msg += '<tr>'
            for item in i:
                msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        sum = 0
        for value in data.get('payment'):
            sum += value[-1]
        msg += '<tr>'
        msg += '<td>{}</td>'.format('总计')
        msg += '<td>{}</td>'.format(sum)
        msg += '</tr>'
        msg += '</table>'
        msg += '<br>'
        
        msg += '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>注册来源</td>'
        msg += '<td>注册数量</td>'
        msg += '</tr>'
        for i in sorted(data.get('register'), key=lambda x:x[-1], reverse=True):
            msg += '<tr>'
            for item in i:
                msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        sum = 0
        for value in data.get('register'):
            sum += value[-1]
        msg += '<tr>'
        msg += '<td>{}</td>'.format('总计')
        msg += '<td>{}</td>'.format(sum)
        msg += '</tr>'
        msg += '</table>'
        msg += '<br>'
        
        msg += '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>订单来源</td>'
        msg += '<td>订单数量</td>'
        msg += '</tr>'
        for i in sorted(data.get('order'), key=lambda x:x[-1], reverse=True):
            msg += '<tr>'
            for item in i:
                msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        sum = 0
        for value in data.get('order'):
            sum += value[-1]
        msg += '<tr>'
        msg += '<td>{}</td>'.format('总计')
        msg += '<td>{}</td>'.format(sum)
        msg += '</tr>'
        msg += '</table>'
        
        return msg

    def send_mail(self):
        data = self.get_data()
        html = self.get_html(data)
        url = 'http://sendplatform.ops.ymatou.cn/api/send'
        data = {'msg_title':'每小时业务数据汇总', 'addresses': json.dumps(['lzh@ymatou.com', 'jiwei@ymatou.com']), 'msg_type': 'mail', 'msg': html}
        r = requests.post(url, data=data)
        print(r.status_code)
        print(r.text)


class ClientMail(Mail):
    def get_data(self):
        result_total = self.client.query('select sum("total") from "client.count.code" where time>now() - 1h group by app')
        result_2 = self.client.query('select sum("2xx") from "client.count.code" where time>now() - 1h group by app')
        result_3 = self.client.query('select sum("3xx") from "client.count.code" where time>now() - 1h group by app')
        result_4 = self.client.query('select sum("4xx") from "client.count.code" where time>now() - 1h group by app')
        result_5 = self.client.query('select sum("5xx") from "client.count.code" where time>now() - 1h group by app')
        result_time = self.client.query('select mean("mean"),mean("geometric_mean"),mean("median"),mean("upper_90") from "client.stats.resp_time" where time>now() - 1h group by app')

        d_total = {}
        d_2 = {}
        d_3 = {}
        d_4 = {}
        d_5 = {}
        d_mean = {}
        d_geometric_mean = {}
        d_median = {}
        d_upper_90 = {}

        for i in result_total.items():
            d_total[i[0][-1]['app']] = list(i[1])[0]['sum'] 

        for i in result_2.items():
            d_2[i[0][-1]['app']] = list(i[1])[0]['sum'] 

        for i in result_3.items():
            d_3[i[0][-1]['app']] = list(i[1])[0]['sum'] 

        for i in result_4.items():
            d_4[i[0][-1]['app']] = list(i[1])[0]['sum'] 

        for i in result_5.items():
            d_5[i[0][-1]['app']] = list(i[1])[0]['sum'] 

        for i in result_time.items():
            data = list(i[1])[0]
            d_mean[i[0][-1]['app']] = int(data.get('mean'))
            d_geometric_mean[i[0][-1]['app']] = int(data.get('mean_1'))
            d_median[i[0][-1]['app']] = int(data.get('mean_2'))
            d_upper_90[i[0][-1]['app']] = int(data.get('mean_3'))

        l = sorted(d_total.items(), key = lambda a:a[-1], reverse=True)[:100]
        d = {}
        for i in l:
            total = d_2.get(i[0], 0) + d_3.get(i[0], 0) + d_4.get(i[0], 0) + d_5.get(i[0], 0)  
            d[i[0]] = [total, d_2.get(i[0], 0), d_3.get(i[0], 0), d_4.get(i[0], 0), d_5.get(i[0], 0), d_mean.get(i[0]), d_geometric_mean.get(i[0]), d_median.get(i[0]), d_upper_90.get(i[0])]

        l = sorted(d.items(), key = lambda a:a[-1][0], reverse=True)
        return l
            
    def get_html(self, data):
        msg = '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>app</td>'
        msg += '<td>total</td>'
        msg += '<td>2xx</td>'
        msg += '<td>3xx</td>'
        msg += '<td>4xx</td>'
        msg += '<td>5xx</td>'
        msg += '<td>平均值</td>'
        msg += '<td>几何平均值</td>'
        msg += '<td>中位数</td>'
        msg += '<td>90线</td>'
        msg += '</tr>'
        for i in data:
            msg += '<tr>'
            app = i[0]
            value = i[1]
            msg += '<td>{}</td>'.format(app)
            for item in value:
                    msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        msg += '</table>'
        msg += '<br><br>'
        return msg

    def send_mail(self):
        data = self.get_data()
        html = self.get_html(data)
        url = 'http://sendplatform.ops.ymatou.cn/api/send'
        data = {'msg_title':'客户端数据报表', 'addresses': json.dumps(['lzh@ymatou.com', 'jiwei@ymatou.com']), 'msg_type': 'mail', 'msg': html}
        r = requests.post(url, data=data)
        print(r.status_code)
        print(r.text)

class AppMail(Mail):
    def get_data(self):
        result_long_time = self.client.query('select sum("value") from "app.long_time" where time>now() - 1h group by app,counter')
        result_pv = self.client.query('select sum("value") from "app.pv" where time>now() - 1h group by app,counter')

        d_long_time = {}
        d_pv = {}

        for i in result_long_time.items():
            app = i[0][-1]['app']
            counter = i[0][-1]['counter']
            if app[-1] == '/' and counter[0] == '/':
                url = app + counter[1:]
            elif app[-1] == '/' or counter[0] == '/':
                url = app + counter
            else:
                url = app + '/' + counter
            value = list(i[1])[0]['sum'] 
            d_long_time[url] = value

        for i in result_pv.items():
            app = i[0][-1]['app']
            counter = i[0][-1]['counter']
            if app[-1] == '/' and counter[0] == '/':
                url = app + counter[1:]
            elif app[-1] == '/' or counter[0] == '/':
                url = app + counter
            else:
                url = app + '/' + counter
            value = list(i[1])[0]['sum'] 
            d_pv[url] = value

        l = sorted(d_long_time.items(), key = lambda a:a[-1], reverse=True)[:200]
        result = []
        for i in l:
            url = i[0]
            long_time = i[1]
            total =  d_pv.get(i[0])
            account = round(i[1]/d_pv.get(i[0])*100, 2)
            line = [url, long_time, total, account]
            result.append(line)

        result_account = []
        for i in d_long_time.items():
            url = i[0]
            long_time = i[1]
            total = d_pv.get(i[0])
            if d_pv.get(i[0]):
                account = round(i[1]/d_pv.get(i[0])*100, 2)
                if total > 500:
                    line = [url, account, long_time, total]
                    result_account.append(line)
        result_account = sorted(result_account, key = lambda a:a[1], reverse=True)[:200]
        data = [result, result_account]
        return data
            
    def get_html(self, data):
        msg = '数据时间:{}:00:00-{}:00:00'.format(time.strftime("%H",time.localtime(time.time()-500)), time.strftime("%H",time.localtime()))
        msg += '<br>'
        msg += '第一个表格按照高耗时(大于500毫秒)数量排序'
        msg += '<br>'
        msg += '第二个表格按照高耗时(大于500毫秒)占比排序(总数大于500)'
        msg += '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>站点</td>'
        msg += '<td>高耗时数量</td>'
        msg += '<td>总数量</td>'
        msg += '<td>高耗时占比</td>'
        msg += '</tr>'
        for i in data[0]:
            msg += '<tr>'
            for item in i:
                msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        msg += '</table>'
        msg += '<br><br>'
        
        msg += '<table border="1" cellspacing="0">'
        msg += '<tr>'
        msg += '<td>站点</td>'
        msg += '<td>高耗时占比</td>'
        msg += '<td>高耗时数量</td>'
        msg += '<td>总数量</td>'
        msg += '</tr>'
        for i in data[1]:
            msg += '<tr>'
            for item in i:
                msg += '<td>{}</td>'.format(item)
            msg += '</tr>'
        msg += '</table>'
        msg += '<br><br>'
        return msg

    def send_mail(self):
        data = self.get_data()
        html = self.get_html(data)
        url = 'http://sendplatform.ops.ymatou.cn/api/send'
        data = {'msg_title':'应用性能数据报表', 'addresses': json.dumps(['lzh@ymatou.com','jiwei@ymatou.com','fp@ymatou.com','liuhuimiao@ymatou.com','zhouzhonglin@xlobo.com','jmtek@ymatou.com','lingyu@ymatou.com','fanlongfu@ymatou.com','shixingming@ymatou.com','duanjuding@ymatou.com']), 'msg_type': 'mail', 'msg': html}
        r = requests.post(url, data=data)
        print(r.status_code)
        print(r.text)

if __name__ == '__main__':
    eventmail = EventMail()
    eventmail.send_mail()
    businessmail = BusinessMail()
    businessmail.send_mail()
    clientmail = ClientMail()
    clientmail.send_mail()
    appmail = AppMail()
    appmail.send_mail()
