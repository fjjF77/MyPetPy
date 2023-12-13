import logging
import os
import threading
from dataclasses import dataclass
import psutil
import subprocess

from Work.Job import JobStatus
from Work.Resource import ResourceInfo

def run_command(cmd):
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return result.stdout.decode()


class ManagerMonitor:
    '''
    定时检查本机资源是否空闲；定时检查WorkHub任务、资源更新情况
    '''
    def __init__(self,workhub_cilent,lock,**kwargs):
        self.workhub_cilent = workhub_cilent
        self.manager_execute_lock = lock
        for k,v in kwargs.items():
            setattr(self,k,v)

        self.checker = threading.Timer(10,self.available)

    def CPU(self):
        cpu_percent = psutil.cpu_percent()
        return cpu_percent
    def GPU(self):
        return 0
    def Users(self):
        users = psutil.users()
        names = [user.name for user in users]
        me = os.getlogin() # 去掉本用户
        names.remove(me)
        return names
    def Process(self):
        # total_processes = psutil.pids() #获取当前总进程数
        # 获取当前用户的进程数
        current_user_processes = [p.pid for p in psutil.process_iter(['pid', 'name', 'username'])]
        current_user_processes_count = len(current_user_processes)
        return current_user_processes_count
    def Status(self):
        cpu = self.CPU()
        gpu = self.GPU()
        users = self.Users()
        process = self.Process()
        resource_info = ResourceInfo(cpu,gpu,users,process)
        return resource_info
    def start(self):
        logging.debug('checker started.')
        self.checker.start()
    def available(self):
        # 资源不足
        logging.info('checker checking...')
        resource_info = self.Status()
        resources, jobs = self.workhub_cilent.workhub_update()
        logging.debug(f'resource:{resource_info}, WorkHub resources:{resources}, WorkHub jobs:{jobs}')
        job_available = False
        for id,job in jobs.items():
            if job.status() == JobStatus.WAITING:
                job_available = True
                break
        # if self.max_process <= self.monitor.Process():
        if len(resource_info.users) == 0 and resource_info.cpu <= self.max_cpu and job_available:  # 无其他人使用 & CPU占用未超限 & 有作业等待
            logging.debug(f'Resource and Jobs are both available. Task, start!')
            self.manager_execute_lock.release()
        else:
            logging.debug(f'Nooooo! It\'s not available!')

    # def CPU(self):
    #     output = run_command('top')
    #     return output
    #
    # def GPU(self):
    #     # output = run_command('nvidia-smi')
    #     output = run_command('gpustat')
    #     return output
    #
    # def Users(self):
    #     output = run_command('who')
    #     return output
    #
    # def Process(self):
    #     output = run_command('top')
    #     return output