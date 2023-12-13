import logging
import socket
import pickle
import json
import threading
import time
import asyncio

from Work.Job import Job
from Work.Resource import Resource
from utils.JsonEncoder import SerializeJsonEncoder

send_over_flag = b'\r\n\r\n' # TCP发送数据包完毕标识

class WorkHubCilent:
    def __init__(self, conn):
        # 客户端连接服务器代理
        self.conn = conn
        # # 定时接收更新资源任务信息
        # ask_t = threading.Thread(target=self.workhub_update,args=())

        # 更新上传自身状态信息

    def send_msg(self, type='ask resources', data=None):
        if data:
            send_data = {'type': type, 'data': data}
        else:
            send_data = {'type': type}
        # send_data = json.dumps(send_data, cls=SerializeJsonEncoder).encode()
        send_data = pickle.dumps(send_data)
        self.conn.sendall(send_data+send_over_flag)

    def recv_msg(self, type='ask resources'):
        if type=='login':
            return
        else:
            # response = self.conn.recv(1024) # 长度为1024，可能会导致接收截断，导致无法反序列化，出现_pickle.UnpicklingError: pickle data was truncated
            response = b''
            while True:
                packet = self.conn.recv(1024)
                if not packet:  # https://www.zhihu.com/question/587806751 TODO:未来可以加入超时取消等解决网络不好时bug的可能
                    break
                if packet.endswith(send_over_flag): # 尾标识判断接收是否完毕
                    packet = packet.decode('latin1').strip(send_over_flag.decode('latin1')).encode('latin1') # 二进制串不能直接使用strip，需要先转成str才能用
                    response += packet
                    break
                response += packet
            # response = json.loads(response.decode())
            response = pickle.loads(response)

        # if response['type'] == 'response resources':
        #     return json.loads(response.data)
        # elif response['type'] == 'response jobs':
        #     return json.loads(response.data)
        # elif response['type'] == 'response upload job':
        #     return json.loads(response.data)
        # elif response.type == 'response assign':
        #     return json.loads(response.data)
        if not response:
            return
        else:
            return response['data']

    def available(self):
        pass

    def workhub_update(self):
        # 接收更新资源任务信息
        self.send_msg(type='ask resources')
        self.send_msg(type='ask jobs')
        resources = self.recv_msg(type='response resources')
        jobs = self.recv_msg(type='response jobs')
        return resources, jobs

    def release(self, job):
        # 发步任务信息
        logging.info(f'WorkHubClient release a job, {job}.')
        self.send_msg(type='upload job', data=job)
        response = self.recv_msg(type='upload job')
        # assert response.type == 'response job id'
        return response['id'] # job id

    def update_job(self, jobs):
        # 更新上传自身任务信息
        self.send_msg(type='update jobs status', data=jobs) #[job.serialize() for job in jobs]
        response = self.recv_msg(type='update job')

    def update_resource(self, resource):
        # 更新上传自身资源状态信息
        self.send_msg(type='update resource status', data=resource)
        response = self.recv_msg(type='update resource')

    def get_job(self):
        # 接收任务信息
        self.send_msg(type='assign job', data='')
        job = self.recv_msg(type='down job')
        return job


class WorkHubServer:
    # 服务器与客户端连接代理
    def __init__(self, conn):
        self.conn = conn

    def send_msg(self,data):
        logging.debug(f'begin send data,{data}.')
        # send_data = json.dumps(data,cls=SerializeJsonEncoder).encode()
        send_data = pickle.dumps(data)
        self.conn.sendall(send_data+send_over_flag)

    def recv_msg(self):
        # msg = self.conn.recv(1024)
        msg = b''
        while True:
            packet = self.conn.recv(1024)
            if not packet:  # https://www.zhihu.com/question/587806751 TODO:未来可以加入超时取消等解决网络不好时bug的可能
                break
            if packet.endswith(send_over_flag):  # 尾标识判断接收是否完毕
                packet = packet.decode('latin1').strip(send_over_flag.decode('latin1')).encode('latin1')  # 二进制串不能直接使用strip，需要先转成str才能用
                msg += packet
                break
            msg += packet

        # data = json.loads(msg.decode())
        data = pickle.loads(msg)
        logging.info(f'WorkHub get a massage, {data}.')
        return data

    def get_resource_msg(self):
        msg = self.recv_msg()
        resource_id = msg['data']['id']
        resource = msg['data']['data']
        return resource
