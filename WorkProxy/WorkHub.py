import socket
import threading
import json
import logging


from Work.Task import Task
from Work.Job import Job, JobStatus
from WorkHubCilent import WorkHubServer
from Work.Resource import ResourceStatus,ResourceInfo
# from Work.Job import JobInfo

class WorkHub:
    def __init__(self):
        self.jobs = {}
        self.job_id = 0
        self.resources = {}
        self.cilents = []

        with open('../Work/resource.json', 'r') as f:
            config = json.load(f)
        self.host = config['WorkHubIP'] # socket.gethostname()
        self.port = int(config['WorkHubPort'])
        self.listen()  # 启动监听，与不同机器通信

    def listen(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:  # 创建套接字
            s.bind((self.host, self.port))

            s.listen(1)  # 开始监听
            logging.info('workhub begin listening...')

            while True:
                conn, addr = s.accept()  # 接受客户端请求，并创建新的套接字对象
                server_conn = WorkHubServer(conn)
                try:
                    resource = server_conn.get_resource_msg()
                    # 接受信息，更新资源列表状态
                    # user_ip = conn.getpeername()[0]
                    # user_name = conn.recv(1024).decode()
                    # resource = Resource(user_ip,user_name)
                    if resource.id in self.resources:
                        resource.status = ResourceStatus.CONNECTING
                        print(f'{resource} login in.')
                    else:
                        self.resources[resource.id]=resource
                        print(f'A new cilent, {resource}, register.')
                    server_conn.id = resource.id

                    # 创建新线程处理信息
                    # send_t = threading.Thread(target=self.send_msg,args=(server_conn,))
                    recv_t = threading.Thread(target=self.recv_msg,args=(server_conn,))
                    self.cilents.append((server_conn,recv_t))
                    logging.info(f'a threading for {resource}.')
                    try:
                        # send_t.start()
                        recv_t.start()
                        # send_t.join()
                        recv_t.join()
                    except Exception as e:
                        raise e
                except Exception as e:
                    logging.warning(e)
                    raise e

    def recv_msg(self,server_conn):
        # 接收客户端信息
        print(f'listening to {server_conn.id}')
        while True:
            msg = server_conn.recv_msg()
            logging.info(f'receive a massage, {msg}')
            if msg['type']=='update resource status':
                # 接受服务器状态更新
                resource_id = msg['id']
                ri = msg['data']
                self.resources[resource_id].info = ri
            elif msg['type']=='update jobs status':
                # 接受执行作业更新（具体为其中的任务执行状态的更新）
                job_id = msg['id']
                ji = msg['data']
                self.jobs[job_id].info = ji
            elif msg['type']=='upload job':
                # 添加新任务
                job = msg['data']
                job.id = self.give_job_id()
                logging.debug(f'a new job uploaded, {job}.')
                self.jobs[job.id] = job
                response = {'type': 'response job id', 'data': {'id':job.id}}
                server_conn.send_msg(response)
            elif msg['type']=='ask resources':
                # 客户端查询资源信息
                resources = self.resources
                response = {'type':'response resources','data':resources} #[resource.serialize() for resource in resources.values()]
                server_conn.send_msg(response)
            elif msg['type']=='ask jobs':
                # 客户端查询任务信息
                jobs = self.jobs
                response = {'type':'response job','data':jobs}
                server_conn.send_msg(response)
            elif msg['type'] == 'assign job':
                # 客户端请求分配任务
                job = self.assign_job()
                response = {'type': 'response assign', 'data': job}
                server_conn.send_msg(response)
            else:
                print(f'unsupported msg.')

    def assign_job(self):
        for id,job in self.jobs.items():
            if job.status() == JobStatus.WAITING:
                job.assigning = True
                return job
        return None

    def give_job_id(self):
        self.job_id += 1
        return self.job_id

    def __del__(self):
        for tup in self.cilents:
            conn,t = tup
            # t.stop()
            conn.close()

if __name__=='__main__':
    # 设置日志级别为DEBUG
    logging.basicConfig(level=logging.DEBUG)

    workhub = WorkHub()

