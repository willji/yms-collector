import pexpect
import datetime
import pymysql
import logging
from ..collectors import BaseCollector
from ..metric import Metric


class MysqlCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)

    def get_ip_port(self):
        conn = None
        cur = None
        result = {}
        try:
            sql = "SELECT `serverip`, `serverport` FROM  dbmonitor.`t_mysqlserverinfo` WHERE  isdeleted=0;"
            conn = pymysql.connect(host=self.context.params.get('host'),
                                   port=self.context.params.get('port'),
                                   user=self.context.params.get('user'),
                                   password=self.context.params.get('password'),
                                   db=self.context.params.get('db'))
            cur = conn.cursor()
            cur.execute(sql)
            result = cur.fetchall()
        except Exception as e:
            logging.error("run sql collector error: {}".format(e))
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            return result


    def ssh_cmd(self, ip, cmd):
        ssh = ssh1 = None
        result = []
        ssh = pexpect.spawn('ssh root@{} {}'.format(ip, cmd))
        try:
            ssh.sendline(cmd)
            r = ssh.readlines()
            ssh.close()
            for i in r:
                j= [item.decode('utf8') for item in i.strip().split()]
                if len(j)>5:
                    lcmd=("netstat -anop|grep {}|grep LISTEN".format(j[0]))
                    ssh1=pexpect.spawn('ssh root@{} {}'.format(ip, lcmd))
                    ssh1.sendline(lcmd)
                    listenport=[i.decode('utf8') for i in ssh1.readlines()]
                    ssh1.close()
                    alist=[]
                    for portloop in listenport:
                        oneport=portloop.strip().split()
                        if oneport[0]=='tcp':
                            if len(oneport)>5 and oneport.index('LISTEN')>=0:
                                spt=oneport[3].split(':')
                                slen=len(spt)
                                if slen>0:
                                    alist.append(eval(spt[slen-1]))
                    if len(alist)>0:
                        minport=min(alist)
                        result.append({"host":ip,"port":int(minport),"cpu":float(j[8])})
        except pexpect.EOF:
            logging.error("{} EOF".format(ip))
        except pexpect.TIMEOUT:
            logging.error("{} TIMEOUT".format(ip))
        finally:
            if ssh:
                ssh.close()
            if ssh1:
                ssh1.close()
            return result

    def make_metric(self, data, mtype):
            tags = {}
            config = self.context.params.get(mtype)
            key = config.get('key')
            value = data.get(config.get('value'))
            for k,v in config.get('tag').items():
                tags[k] = data.get(v)
            timestamp = datetime.datetime.now()
            return Metric(key, value, tags=tags, timestamp=timestamp)

    def get_diffsec(self, info):
        conn = None
        cur = None
        result = {}
        try:
            sql = "SELECT TIMESTAMPDIFF(SECOND,c_currtime,now()) as diffsec from t_dba_timediff"
            host = info[0]
            port = int(info[1])
            conn = pymysql.connect(host=host,
                                   port=port,
                                   user=self.context.params.get('user', 'root'),
                                   password=self.context.params.get('password'),
                                   db='dbmanager')
            cur = conn.cursor()
            cur.execute(sql)
            result = {'diffsec': cur.fetchone()[0], 'host': host, 'port': port}
        except Exception as e:
            logging.error("run sql collector error: {}".format(e))
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            return result


    def run_with_mysql(self):
        metrics = []
        conn = None
        cur = None
        try:
            conn = pymysql.connect(host=self.context.params.get('host', 'localhost'),
                                   port=self.context.params.get('port', 3306),
                                   user=self.context.params.get('user', 'root'),
                                   password=self.context.params.get('password'),
                                   db=self.context.params.get('db'))
            cur = conn.cursor()
            
            query = "select distinct serverip from dbmonitor.t_mysqlserverinfo where isdeleted=0;"
            cur.execute(query)
            iplist=cur.fetchall()
            lcmd="/usr/bin/top -b -n 1|grep mysqld"
            for sip in iplist:
                for ssip in sip:
                    for data in self.ssh_cmd(str(ssip),lcmd):
                        metrics.append(self.make_metric(data, 'cpu'))
            
            ip_port = self.get_ip_port()
            for info in ip_port:
                data = self.get_diffsec(info)
                if data:
                    metrics.append(self.make_metric(data, 'diffsec'))

        except Exception as e:
            logging.error("run sql collector error: {}".format(e))
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            return metrics

    def run(self):
        return self.run_with_mysql()
