import pexpect
import datetime
import pymysql
import logging
from ..collectors import BaseCollector
from ..metric import Metric
from pymongo import MongoClient


class MongoCollector(BaseCollector):
    def __init__(self, context):
        super().__init__(context)

    def ssh_cmd(self, ip, cmd):
        ssh = ssh1 = None
        result = []
        ssh = pexpect.spawn('ssh root@{} {}'.format(ip, cmd))
        try:
            ssh.sendline(cmd)
            r = ssh.readlines()
            for i in r:
                j= [item.decode('utf8') for item in i.strip().split()]
                if len(j)>5:
                    lcmd=("netstat -anop|grep {}|grep LISTEN".format(j[0]))
                    ssh1=pexpect.spawn('ssh root@{} {}'.format(ip, lcmd))
                    ssh1.sendline(lcmd)
                    listenport=[i.decode('utf8') for i in ssh1.readlines()]
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
    
    def getMongoConn(self, vconnectstr, isauth, authtype):
        try:
            connectstr = vconnectstr
            client = MongoClient("mongodb://%s"%connectstr, socketTimeoutMS=20000)
            if isauth == 1:
                if vconnectstr.split(':')[0] in ['10.12.0.85', '10.12.0.86']:
                    pswd = self.context.params.get('mongo_password')
                    client.admin.authenticate('sa', pswd)
                else:
                    pswd = self.context.params.get('mongo_password')
                    client.admin.authenticate('sa', pswd, mechanism=authtype)
            db = client.get_database(name='admin')
            xdct = db.command("serverStatus")
            host, port = vconnectstr.split(':')
            return {"host":host, "port":port, "connection":xdct['connections']['current']}
        except Exception as e:
            logging.error("{} {}".format(vconnectstr, e))
        finally:
            client.close()

    def make_metric(self, data, mtype):
            tags = {}
            config = self.context.params.get(mtype)
            key = config.get('key')
            value = data.get(config.get('value'))
            for k,v in config.get('tag').items():
                tags[k] = data.get(v)
            timestamp = datetime.datetime.now()
            return Metric(key, value, tags=tags, timestamp=timestamp)


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
             
            query = "select distinct serverip from dbmonitor.t_mongoserverinfo;"
            cur.execute(query)
            iplist=cur.fetchall()
            lcmd="/usr/bin/top -b -n 1|grep mongod"
            for sip in iplist:
                for ssip in sip:
                    for data in self.ssh_cmd(str(ssip),lcmd):
                        metrics.append(self.make_metric(data, 'cpu'))
            
            mlistsql="select surl,isauth,authtype from dbmonitor.t_mongoserverinfo;"
            cur.execute(mlistsql)
            ret=cur.fetchall()
            mongolist = []
            for i in ret:
                mongolist.append(i)
            for sq in mongolist:
                currconn = self.getMongoConn(sq[0],sq[1],sq[2])
                if currconn:
                    metrics.append(self.make_metric(currconn, 'connection'))

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
