import time
import pymysql
from elasticsearch import Elasticsearch
from ..collectors import BaseCollector
from ..metric import Metric


class LogCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)
        self.es = Elasticsearch(hosts=[{'host': self.context.params.get('es_host1', 'localhost'), 'port': self.context.params.get('es_port', 9200)}, {'host': self.context.params.get('es_host2', 'localhost'), 'port': self.context.params.get('es_port', 9200)}, {'host'                                          : self.context.params.get('es_host3', 'localhost'), 'port': self.context.params.get('es_port', 9200)}])

    def get_mysql_result(self, sql):
        try:    
            conn = pymysql.connect(host=self.context.params.get('db_host', 'localhost'),user=self.context.params.get('db_user', 'root'),\
                    passwd=self.context.params.get('db_password', ''),db=self.context.params.get('db_db', 'fulllink'),port=3306,charset='utf8')
            cur = conn.cursor()
            cur.execute(sql)
            data = cur.fetchall()
            return data
        except Exception as e:
            raise e 
        finally:
            cur.close()
            conn.close()

    def get_forward(self, domain):
        sql = "select `path`,`app` from `api_forward` where `domain`='{0}';".format(domain)
        r = self.get_mysql_result(sql=sql)
        data = {}
        for i in r:
            data[i[0]] = i[1]
        return data
    
    def get_mainapps(self):
        sql = "select `name` from `api_dependent` where `type`='mainApp';"
        r = self.get_mysql_result(sql=sql)
        data = []
        for i in r:
            data.append(i[0])
        data = list(set(data))
        return data

    def get_jumpapps(self):
        sql = "select `name` from `api_dependent` where `type`='jumpApp';"
        r = self.get_mysql_result(sql=sql)
        data = []
        for i in r:
            data.append(i[0])
        data = list(set(data))
        return data

    def get_apps(self):
        sql = "select `name` from `api_dependent` where `type`='app';"
        r = self.get_mysql_result(sql=sql)
        data = []
        for i in r:
            data.append(i[0])
        data = list(set(data))
        return data
    
    def get_index(self, type='nginx'):
        ISOTIMEFORMAT='%Y.%m.%d'
        return type + '-' + time.strftime(ISOTIMEFORMAT, time.localtime(time.time()-28800))

    def get_nginx_code(self, index, bool_value, from_time, to_time):
        res = self.es.search(index=index, doc_type='nginx-log', body=
    {
        "size" : 0,
        "query":{
        "bool": {
            bool_value:[
                {"match": {"client_ip": "127.0.0.1"}}
            ]   
            }   
        },  
        "aggs":{
            "recent_time": {
                "filter": { 
                    "range": {
                        "@timestamp": {
                            "from": from_time, "to" :to_time
                        }   
                    }   
                },  
        "aggs": {
            "group_by_code": {
                "range": {
                    "field": 'response',
                    "ranges": [
                    {   
                        "key": "2XX",
                        "from": 200,
                        "to": 300 
                    },
                    {
                        "key": "3XX",
                        "from": 300,
                        "to": 400
                    },
                    {
                        "key": "4XX",
                        "from": 400,
                        "to": 500
                    },
                    {
                        "key": "5XX",
                        "from": 500,
                        "to": 600
                    }
                    ]
                },
                "aggs": {
                    "group_by_domain": {
                        "terms": {
                            "size" : 1000, "field": 'domain'
                        }
                    }
                }
            }
            }
            }
        }
    }
    )
        return res['aggregations']['recent_time']['group_by_code']['buckets']

    def get_nginx_time(self, index, bool_value, from_time, to_time):
        res = self.es.search(index=index, doc_type='nginx-log', body=
    {
       "size" : 0,
       "query":{
        "bool": {
           bool_value:[
            {"match": {"client_ip": "127.0.0.1"}}
        ]   
        }   
       },  
       "aggs":{
          "recent_time": {
             "filter": { 
                "range": {
                   "@timestamp": {
                      "from": from_time, "to" :to_time
                   }   
                }   
             },  
            "aggs" : { 
                "group_by_domain": {
                    "terms" : { 
                        "size" : 1000, "field" : 'domain'
                    },  
                    "aggs" : { 
                        "average_request_time" : { 
                            "avg" : { 
                                "field": 'request_time'
                            }   
                        },  
                        "average_response_time" : { 
                            "avg" : { 
                                "field": 'upstream_response_time'
                            }
                        }
                    }
                }
            }
          }
       }
    }
    )
        return res['aggregations']['recent_time']['group_by_domain']['buckets']

    def get_forward_code(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='nginx-log', body=
    {
        "size" : 10,
        "query":{
        "bool": {
            "must":[
                {"match": {"client_ip": "127.0.0.1"}},
                {"match": {"domain": "app.ymatou.com"}}
            ]   
            }   
        },  
        "aggs":{
            "recent_time": {
                "filter": { 
                    "range": {
                        "@timestamp": {
                            "from": from_time, "to" :to_time
                        }   
                    }   
                },  
        "aggs": {
            "group_by_code": {
                "range": {
                    "field": 'response',
                    "ranges": [
                    {   
                        "key": "2XX",
                        "from": 200,
                        "to": 300 
                    },
                    {
                        "key": "3XX",
                        "from": 300,
                        "to": 400
                    },
                    {
                        "key": "4XX",
                        "from": 400,
                        "to": 500
                    },
                    {
                        "key": "5XX",
                        "from": 500,
                        "to": 600
                    }
                    ]
                },
                "aggs": {
                    "group_by_request": {
                        "terms": {
                            "size" : 1000, "field": 'request'
                        }
                    }
                }
            }
            }
            }
        }
    }
    )
        return res['aggregations']['recent_time']['group_by_code']['buckets']

    def get_forward_time(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='nginx-log', body=
    {
       "size" : 0,
       "query":{
        "bool": {
           "must":[
            {"match": {"client_ip": "127.0.0.1"}},
            {"match": {"domain": "app.ymatou.com"}}
        ]   
        }   
       },  
       "aggs":{
          "recent_time": {
             "filter": { 
                "range": {
                   "@timestamp": {
                      "from": from_time, "to" :to_time
                   }   
                }   
             },  
            "aggs" : { 
                "group_by_request": {
                    "terms" : { 
                        "size" : 1000, "field" : 'request'
                    },  
                    "aggs" : { 
                        "average_request_time" : { 
                            "avg" : { 
                                "field": 'request_time'
                            }   
                        },  
                        "average_response_time" : { 
                            "avg" : { 
                                "field": 'upstream_response_time'
                            }
                        }
                    }
                }
            }
          }
       }
    }
    )
        return res['aggregations']['recent_time']['group_by_request']['buckets']

    def get_netscaler_code(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='netscaler-log', body=
    {
        "query": {
            "range" : { 
                "@timestamp" : { 
                    "from" : from_time, "to" : to_time
                } 
            }
        },
        "size" : 0,
        "aggs": {
            "group_by_code": {
                "range": {
                    "field": 'ns_response',
                    "ranges": [
                    {
                        "key": "2XX",
                        "from": 200,
                        "to": 300
                    },
                    {
                        "key": "3XX",
                        "from": 300,
                        "to": 400
                    },
                    {
                        "key": "4XX",
                        "from": 400,
                        "to": 500
                    },
                    {
                        "key": "5XX",
                        "from": 500,
                        "to": 600
                    }
                    ]
                },
                "aggs": {
                    "group_by_domain": {
                        "terms": {
                            "size" : 1000, "field": 'ns_domain'
                        }
                    }
                }
            }
        }
    }
    )
        return res['aggregations']['group_by_code']['buckets']

    def get_netscaler_time(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='netscaler-log', body=
    {
        "query": {
            "range" : { 
                "@timestamp" : { 
                    "from" : from_time, "to" : to_time 
                } 
            }
        },
        "size" : 0,
        "aggs" : {
            "group_by_domain": {
                "terms" : {
                    "size" : 1000, "field" : "ns_domain"
                },
                "aggs" : {
                    "average_server_time" : {
                        "avg" : {
                            "field":"ns_servertime" 
                        }
                    }
                }
            }
        }
    }
    )
        return res['aggregations']['group_by_domain']['buckets']

    def get_haproxy_time(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='haproxy-log', body=
    {
        "query": {
            "range" : { 
                "@timestamp" : { 
                    "from" : from_time, "to" : to_time
                } 
            }
        },
        "size" : 0,
        "aggs" : {
            "group_by_domain": {
                "terms" : {
                   "size" : 1000, "field" : 'ha_domain'
                },
                "aggs" : {
                    "average_request_time" : {
                        "avg" : {
                            "field": "ha_requesttime"  
                        }
                    },
                    "average_response_time" : {
                        "avg" : {
                            "field": "ha_responsetime"
                        }
                    }
                }
            }
        }
    }
    )
        return res['aggregations']['group_by_domain']['buckets']

    def get_haproxy_code(self, index, from_time, to_time):
        res = self.es.search(index=index, doc_type='haproxy-log', body=
    {
        "query": {
            "range" : { 
                "@timestamp" : { 
                    "from" : from_time, "to" : to_time
                } 
            }
        },
        "size" : 0,
        "aggs": {
            "group_by_code": {
                "range": {
                    "field": 'ha_response',
                    "ranges": [
                    {
                        "key": "2XX",
                        "from": 200,
                        "to": 300
                    },
                    {
                        "key": "3XX",
                        "from": 300,
                        "to": 400
                    },
                    {
                        "key": "4XX",
                        "from": 400,
                        "to": 500
                    },
                    {
                        "key": "5XX",
                        "from": 500,
                        "to": 600
                    }
                    ]
                },
                "aggs": {
                    "group_by_domain": {
                        "terms": {
                            "size" : 1000, "field": 'ha_domain'
                        }
                    }
                }
            }
        }
    }
    )
        return res['aggregations']['group_by_code']['buckets']

    def data_convergence(self, tmp_time_data, tmp_code_data, log_type=None):
        data = []
        time_data = {}
        code_data = {}
        for i in tmp_time_data:
            if log_type == 'netscaler':
                time_data[i['key']] = {'average_server_time':i['average_server_time']['value']}
            else:
                time_data[i['key']] = {'average_request_time':i['average_request_time']['value'], 'average_response_time':i['average_response_time']['value']}
        for i in tmp_code_data:
            for j in i['group_by_domain']['buckets']:
                if j['key'] in code_data:
                    code_data[j['key']][i['key']] = j['doc_count']
                else:
                    code_data[j['key']] = {i['key']:j['doc_count']}
        for i in time_data:
            if log_type == 'netscaler':
                tmp_data = {'domain':i, 'average_server_time':time_data[i]['average_server_time']}
            else:
                tmp_data = {'domain':i, 'average_request_time':time_data[i]['average_request_time'], 'average_response_time':time_data[i]['average_response_time']}
            if i in code_data:
                tmp_data['2XX'] = code_data[i].get('2XX', 0)
                tmp_data['3XX'] = code_data[i].get('3XX', 0)
                tmp_data['4XX'] = code_data[i].get('4XX', 0)
                tmp_data['5XX'] = code_data[i].get('5XX', 0)
            data.append(tmp_data)
        return data

    def get_netscaler_data(self, index, from_time, to_time):
        tmp_netscaler_time_data = self.get_netscaler_time(index, from_time=from_time, to_time=to_time)
        tmp_netscaler_code_data = self.get_netscaler_code(index, from_time=from_time, to_time=to_time)
        log_data = self.data_convergence(tmp_time_data = tmp_netscaler_time_data, tmp_code_data = tmp_netscaler_code_data, log_type='netscaler')
        for d in log_data:
            if not d.get('average_server_time'):
                d['average_server_time'] = 0
            else:
                d['average_server_time'] = round(d['average_server_time'], 3)
        tmp_apps = [i['domain'] for i in log_data]
        apps = self.get_mainapps()
        for i in apps:
            if i not in tmp_apps:
                log_data.append({'domain': i})
        return log_data

    def get_nginx_data(self, index, from_time, to_time, nginx_type):
        if nginx_type == 'intranet':
            tmp_nginx_time_data = self.get_nginx_time(index, bool_value='must_not', from_time=from_time, to_time=to_time)
            tmp_nginx_code_data = self.get_nginx_code(index, bool_value='must_not', from_time=from_time, to_time=to_time)
        else:
            tmp_nginx_time_data = self.get_nginx_time(index, bool_value='must', from_time=from_time, to_time=to_time)
            tmp_nginx_code_data = self.get_nginx_code(index, bool_value='must', from_time=from_time, to_time=to_time)
        log_data = self.data_convergence(tmp_time_data = tmp_nginx_time_data, tmp_code_data = tmp_nginx_code_data)
        for d in log_data:
            if not d.get('average_request_time'):
                d['average_request_time'] = 0
            else:
                d['average_request_time'] = round(d['average_request_time'], 3)
            if not d.get('average_response_time'):
                d['average_response_time'] = 0
            else:
                d['average_response_time'] = round(d['average_response_time'], 3)
        if nginx_type == 'extranet':
            tmp_apps = [i['domain'] for i in log_data]
            apps = self.get_mainapps()
            for i in apps:
                if i not in tmp_apps:
                    log_data.append({'domain': i})
        else:
            tmp_apps = [i['domain'] for i in log_data]
            apps = self.get_apps()
            for i in apps:
                if i not in tmp_apps:
                    log_data.append({'domain': i})
        return log_data

    def get_haproxy_data(self, index,from_time, to_time):
        tmp_haproxy_time_data = self.get_haproxy_time(index, from_time=from_time, to_time=to_time)
        tmp_haproxy_code_data = self.get_haproxy_code(index, from_time=from_time, to_time=to_time)
        log_data = self.data_convergence(tmp_time_data = tmp_haproxy_time_data, tmp_code_data = tmp_haproxy_code_data)
        for d in log_data:
            if not d.get('average_request_time'):
                d['average_request_time'] = 0
            else:
                d['average_request_time'] = round(d['average_request_time']/1000.0, 3)
            if not d.get('average_response_time'):
                d['average_response_time'] = 0
            else:
                d['average_response_time'] = round(d['average_response_time']/1000.0, 3)
        tmp_apps = [i['domain'] for i in log_data]
        apps = self.get_apps()
        for i in apps:
            if i not in tmp_apps:
                log_data.append({'domain': i})
        return log_data

    def replace_time_data(self, forward_time):
        forward_table = self.get_forward('app.ymatou.com')
        tmp_data = []
        for k in forward_table:
            for i in forward_time:
                if k in i['key']:
                    tmp_data.append({'domain':forward_table[k], 'count':i['doc_count'], 'average_request_time':i['average_request_time']['value'], 'average_response_time':i['average_response_time']['value']})
        data = {}
        for i in tmp_data:
            if i['domain'] in data:
                count = i['count'] + data[i['domain']]['count']
                average_request_time = i['average_request_time']*i['count'] + data[i['domain']]['average_request_time']
                average_response_time = i['average_response_time']*i['count'] + data[i['domain']]['average_response_time']
                data[i['domain']] = {'average_request_time':average_request_time,'average_response_time':average_response_time,'count':count}
            else:
                data[i['domain']] = {'average_request_time':i['average_request_time']*i['count'],'average_response_time':i['average_response_time']*i['count'],'count':i['count']}
        for k in data:
            count = data[k]['count']
            average_request_time = round(data[k]['average_request_time'] / count, 3)
            average_response_time = round(data[k]['average_response_time'] / count, 3)
            data[k] = {'count':count, 'average_request_time':average_request_time, 'average_response_time':average_response_time}
        return data

    def replace_code_data(self, forward_code):
        forward_table = self.get_forward('app.ymatou.com')
        tmp_data = []
        for k in forward_table:
            for j in range(4):
                for i in forward_code[j]['group_by_request']['buckets']:
                    code = str(j+2) + 'XX'
                    if k in i['key']:
                        tmp_data.append({'domain':forward_table[k], code:i['doc_count']})
        data = {}
        for i in tmp_data:
            if i['domain'] in data:
                XX2 = i.get('2XX', 0) + data[i['domain']]['2XX'] 
                XX3 = i.get('3XX', 0) + data[i['domain']]['3XX'] 
                XX4 = i.get('4XX', 0) + data[i['domain']]['4XX'] 
                XX5 = i.get('5XX', 0) + data[i['domain']]['5XX'] 
                data[i['domain']] = {'2XX':XX2,'3XX':XX3,'4XX':XX4,'5XX':XX5}
            else:
                data[i['domain']] = {'2XX':i.get('2XX', 0),'3XX':i.get('3XX', 0),'4XX':i.get('4XX', 0),'5XX':i.get('5XX', 0)}
        return data

    def get_forward_data(self, index, from_time, to_time):
        forward_code = self.get_forward_code(index=index, from_time=from_time, to_time=to_time)
        forward_time = self.get_forward_time(index=index, from_time=from_time, to_time=to_time)
        code_data = self.replace_code_data(forward_code)
        time_data = self.replace_time_data(forward_time)
        log_data = []
        for i in time_data:
            tmp_data = {'domain':i, 'average_request_time':time_data[i]['average_request_time'], 'average_response_time':time_data[i]['average_response_time']}
            if i in code_data:
                tmp_data['2XX'] = code_data[i].get('2XX', 0)
                tmp_data['3XX'] = code_data[i].get('3XX', 0)
                tmp_data['4XX'] = code_data[i].get('4XX', 0)
                tmp_data['5XX'] = code_data[i].get('5XX', 0)
            log_data.append(tmp_data)
        tmp_apps = [i['domain'] for i in log_data]
        apps = self.get_jumpapps()
        for i in apps:
            if i not in tmp_apps:
                log_data.append({'domain': i})
        return log_data

    def run(self):
        metrics = []
        index = self.get_index('nginx')
        #netscaler_data = self.get_netscaler_data(index, 'now-70s', 'now-10s')
        intranet_nginx_data = self.get_nginx_data(index, 'now-70s', 'now-10s', nginx_type='intranet')
        extranet_nginx_data = self.get_nginx_data(index, 'now-70s', 'now-10s', nginx_type='extranet')
        forward_data = self.get_forward_data(index, 'now-70s', 'now-10s')
        index = self.get_index('haproxy')
        haproxy_data = self.get_haproxy_data(index, 'now-70s', 'now-10s')
        #for i in netscaler_data:
        #    tags = {'app': i.get('domain'), 'type': 'netscaler'}
        #    metrics.append(Metric('average_server_time', i.get('average_server_time', 0), tags=tags))
        #    metrics.append(Metric('2XX', i.get('2XX', 0), tags=tags))
        #    metrics.append(Metric('3XX', i.get('3XX', 0), tags=tags))
        #    metrics.append(Metric('4XX', i.get('4XX', 0), tags=tags))
        #    metrics.append(Metric('5XX', i.get('5XX', 0), tags=tags))
        #    pv = i.get('2XX', 0) + i.get('3XX', 0) + i.get('4XX', 0) + i.get('5XX', 0)
        #    metrics.append(Metric('pv', pv, tags=tags))
        for i in intranet_nginx_data:
            tags = {'app': i.get('domain'), 'type': 'intranet_nginx'}
            metrics.append(Metric('average_request_time', i.get('average_request_time', 0), tags=tags))
            metrics.append(Metric('average_response_time', i.get('average_response_time', 0), tags=tags))
            metrics.append(Metric('2XX', i.get('2XX', 0), tags=tags))
            metrics.append(Metric('3XX', i.get('3XX', 0), tags=tags))
            metrics.append(Metric('4XX', i.get('4XX', 0), tags=tags))
            metrics.append(Metric('5XX', i.get('5XX', 0), tags=tags))
            pv = i.get('2XX', 0) + i.get('3XX', 0) + i.get('4XX', 0) + i.get('5XX', 0)
            metrics.append(Metric('pv', pv, tags=tags))
        for i in extranet_nginx_data:
            tags = {'app': i.get('domain'), 'type': 'extranet_nginx'}
            metrics.append(Metric('average_request_time', i.get('average_request_time', 0), tags=tags))
            metrics.append(Metric('average_response_time', i.get('average_response_time', 0), tags=tags))
            metrics.append(Metric('2XX', i.get('2XX', 0), tags=tags))
            metrics.append(Metric('3XX', i.get('3XX', 0), tags=tags))
            metrics.append(Metric('4XX', i.get('4XX', 0), tags=tags))
            metrics.append(Metric('5XX', i.get('5XX', 0), tags=tags))
            pv = i.get('2XX', 0) + i.get('3XX', 0) + i.get('4XX', 0) + i.get('5XX', 0)
            metrics.append(Metric('pv', pv, tags=tags))
        for i in forward_data:
            tags = {'app': i.get('domain'), 'type': 'extranet_nginx'}
            metrics.append(Metric('average_request_time', i.get('average_request_time', 0), tags=tags))
            metrics.append(Metric('average_response_time', i.get('average_response_time', 0), tags=tags))
            metrics.append(Metric('2XX', i.get('2XX', 0), tags=tags))
            metrics.append(Metric('3XX', i.get('3XX', 0), tags=tags))
            metrics.append(Metric('4XX', i.get('4XX', 0), tags=tags))
            metrics.append(Metric('5XX', i.get('5XX', 0), tags=tags))
            pv = i.get('2XX', 0) + i.get('3XX', 0) + i.get('4XX', 0) + i.get('5XX', 0)
            metrics.append(Metric('pv', pv, tags=tags))
        for i in haproxy_data:
            tags = {'app': i.get('domain'), 'type': 'haproxy'}
            metrics.append(Metric('average_request_time', i.get('average_request_time', 0), tags=tags))
            metrics.append(Metric('average_response_time', i.get('average_response_time', 0), tags=tags))
            metrics.append(Metric('2XX', i.get('2XX', 0), tags=tags))
            metrics.append(Metric('3XX', i.get('3XX', 0), tags=tags))
            metrics.append(Metric('4XX', i.get('4XX', 0), tags=tags))
            metrics.append(Metric('5XX', i.get('5XX', 0), tags=tags))
            pv = i.get('2XX', 0) + i.get('3XX', 0) + i.get('4XX', 0) + i.get('5XX', 0)
            metrics.append(Metric('pv', pv, tags=tags))
        return metrics
