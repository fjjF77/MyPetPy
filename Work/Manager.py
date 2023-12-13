import logging
import os
import json
import socket
import threading
from time import sleep

from utils.JsonEncoder import SerializeJsonEncoder
from Work.Job import ParallelTaskJob, DependentTaskJob, JobStatus
from Work.Task import Task, TaskStatus
from Work.MyThread import MyThread
from WorkProxy.WorkHubCilent import WorkHubCilent
from Monitor.Monitor import ManagerMonitor
from Work import Job
from Work.Resource import Resource

def get_local_IP_User():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('127.0.0.1', 80)) #8.8.8.8
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip,os.getlogin()

class Manager:
    def __init__(self):
        # 读取配置文件
        with open('Work/resource.json', 'r') as f:
            self.config = json.load(f)
        # self.max_process = self.config['max_process']  # 最大进程数
        self.max_cpu = self.config['max_cpu']  # 最大CPU占用
        self.run_mode = self.config['run_mode']  # 运行模式 'normal','silent'
        # 本机资源管理，创立进程/线程池，准备放入任务
        self.thread_pool = []
        self.thread_num = 0
        # 获取本机IP与用户
        self.ip,self.user = get_local_IP_User()
        self.resource = Resource(self.ip,self.user)
        # 加入集群
        self.workhub_ip = self.config['WorkHubIP']
        self.workhub_port = int(self.config['WorkHubPort'])
        self._join_cluster()
        # 设置监控，初始化监控，汇总汇报任务执行信息与资源使用情况
        self.execute_lock = threading.Lock()
        monitor_constrain = {'max_cpu':self.max_cpu}
        self.monitor = ManagerMonitor(self.workhub_cilent,self.execute_lock,**monitor_constrain)

    def _join_cluster(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        while True:
            result = client_socket.connect_ex((self.workhub_ip, self.workhub_port))
            if result == 0:
                logging.info('connect to the workhub.')
                break
        self.workhub_cilent = WorkHubCilent(client_socket)
        print('find WorkHub.')
        # 上传资源信息
        msg = {'id':self.resource.id,'data':self.resource}
        self.workhub_cilent.send_msg(type='login',data=msg)

        # # 获得WorkHub地址
        # WorkHubIP = self.config['WorkHubIP']
        # # 与WorkHub建立连接，获得当前任务与资源信息
        # self.workhub_cilent = None
        # if WorkHubIP == '127.0.0.1': # 若为本机IP，则默认workhub没开，开启workhub
        #     self.workhub_cilent = WorkHubCilent(self.workhub_cilent)
        # else:
        #     self.workhub
        # # 返回该连接（周期性刷新获取信息）？

    def release(self, job):
        logging.info(f'release a job, {job}.')
        self.workhub_cilent.release(job)


    def start(self):
        # 持续监听workhub，若有任务并且空闲则可执行
        logging.debug(f'monitor begin listening.')
        self.monitor.start()
        # 调度作业
        self.execute_lock.acquire() #先锁住不让执行作业，直到monitor确认满足条件
        self.schedule_timer = threading.Timer(5,self.schedule)
        self.schedule_timer.start()

    def schedule(self):  # 调度作业
        with self.execute_lock:
            logging.info('Waiting for WorkHub scheduling...')
            job = self.workhub_cilent.get_job()  # 作业调度，获取作业
            if job:
                self.schedule_job(job)

    def schedule_job(self, job):
        # 作业内调度
        logging.info(f'Manager schedule a job. {job}')
        if isinstance(job, ParallelTaskJob):
            # 并行任务作业直接执行就行
            if job.run_mode == 'all tasks':
                # 同时运行所有任务，改如何控制资源使用？
                run_task_num = 0
                job.status = JobStatus.RUNNING
                for task in job.task_lst:
                    if task.status == TaskStatus.WAITING or task.status == TaskStatus.SUSPENDED:  # 任务正在等待或挂起
                        self.execute(task)  # 执行作业
                        # run_task_num += 1
                        # if run_task_num >= self.max_process:
                        #     break
            elif job.run_mode == 'all_devices':
                # 占满所有卡
                pass
        else:
            # 依赖任务作业可能需要再进行调度
            pass

    def execute(self, task: Task):
        logging.info(f'Execute a task. {task}')
        t = MyThread(task)
        try:
            t.start()
            task.status = TaskStatus.RUNNING
            self.thread_pool.append((task.tid, t))
            t.join()
        except Exception as e:
            task.exception = e
            raise e

    def kill(self, work_name):
        # 任务停止执行
        pass

    def suspend(self, work_name):
        # 任务挂起
        pass
