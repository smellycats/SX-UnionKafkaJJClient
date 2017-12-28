# -*- coding: utf-8 -*-
import time
import json
import socket

import arrow

from helper_consul import ConsulAPI
from helper_kafka_consumer import KafkaConsumer
from helper_unionkk import UnionKakou
from my_yaml import MyYAML
from my_logger import *


debug_logging('/home/logs/error.log')
logger = logging.getLogger('root')


class UploadData(object):
    def __init__(self):
        # 配置文件
        self.my_ini = MyYAML('/home/my.yaml').get_ini()

        # request方法类
        self.kc = None
        self.uk = None
        self.con = ConsulAPI()

        self.uuid = None                    # session id
        self.session_time = time.time()     # session生成时间戳
        self.ttl = dict(self.my_ini['consul'])['ttl']               # 生存周期
        self.lock_name = dict(self.my_ini['consul'])['lock_name']   # 锁名

        self.local_ip = socket.gethostbyname(socket.gethostname())  # 本地IP

        self.partitions = (48, 8)       # 分区数
        self.item = None
        self.part_list = []


    def get_lock(self):
        """获取锁"""
        # 是否打印信息
        p = False
        if self.uuid is None:
            self.uuid = self.con.put_session(self.ttl, self.lock_name)['ID']
            self.session_time = time.time()
            p = True
        # 大于一定时间间隔则更新session
        if (time.time() - self.session_time) > (self.ttl - 5):
            self.con.renew_session(self.uuid)
            self.session_time = time.time()
            p = True
        if self.item is None:
            for i in range(self.partitions[1]):
                l = self.con.get_lock(self.uuid, self.local_ip, i)
                if l == None:
                    self.uuid = None
                    return False
                if l:           # l是True
                    self.item = i
                    self.part_list = list(range(self.partitions[0]))[i::self.partitions[1]]
                    break
        else:
            l = self.con.get_lock(self.uuid, self.local_ip, self.item)
        if p:
            lock_msg = '{0} {1} {2} {3}'.format(self.uuid, l, self.item, self.part_list)
            print(lock_msg)
            logger.info(lock_msg)
        # session过期
        if l == None:
            self.uuid = None
            return False
        return l

    def get_service(self, service):
        """获取服务信息"""
        s = self.con.get_service(service)
        if len(s) == 0:
            return None
        h = self.con.get_health(service)
        if len(h) == 0:
            return None
        service_status = {}
        for i in h:
            service_status[i['ServiceID']] = i['Status']
        for i in s:
            if service_status[i['ServiceID']] == 'passing':
                return {'host': i['ServiceAddress'], 'port': i['ServicePort']}
        return None

    def upload_data(self):
        items = []
        offsets = {}
        for i in range(200):
            msg = self.kc.c.poll(0.005)
        
            if msg is None:
                continue
            if msg.error():
                continue
            else:
                m = json.loads(msg.value().decode('utf-8'))['message']
                m['img_path'] = m['imgurl']
                m['fxbh'] = m['fxbh_id']
                items.append(m)
            par = msg.partition()
            off = msg.offset()
            offsets[par] = off
        if offsets == {}:
            return
        else:
            print('items={0}'.format(len(items)))
            logger.info('items={0}'.format(len(items)))
            if len(items) > 0:
                self.uk.post_kakou(items)                 # 上传数据
            self.kc.c.commit(async=False)
            print(offsets)
            logger.info(offsets)
            return

    def main_loop(self):
        while 1:
            try:
                if not self.get_lock():
                    if self.kc is not None:
                        del self.kc
                        self.kc = None
                    self.item = None
                    time.sleep(2)
                    continue
                if self.kc is None:
                    self.kc = KafkaConsumer(**dict(self.my_ini['kafka']))
                    self.kc.assign(self.part_list)
                if self.uk is not None and self.uk.status:
                    self.upload_data()
                else:
                    s = self.get_service('unionUploadServer')
                    if s is None:
                        time.sleep(5)
                        continue
                    self.uk = UnionKakou(**{'host': s['host'], 'port': s['port']})
                    self.uk.status = True
            except Exception as e:
                logger.exception(e)
                time.sleep(15)

        
