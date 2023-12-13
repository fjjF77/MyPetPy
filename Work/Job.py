import json
from dataclasses import dataclass
from enum import Enum

from Work.Task import Task, TaskStatus


class JobStatus(Enum):
    WAITING = 'WAI'
    RUNNING = 'RUN'
    ASSIGNING = 'ASS' # 正在分配作业
    FINISHED = 'FIN'

    def serialize(self):
        return self.value

class Job:
    def __init__(self, max_process=-1):
        # max_process 是作业使用最大进程数，-1为全部
        self.max_process :int = max_process
        self.cur_process :int = 0
        self.task_lst :list[Task] = []  # 任务列表，TODO:在向Workhub上传时需要先打包成镜像，在Workhub分配作业时下载镜像，并转化成可执行函数进行执行。
        self.class_type :str = __class__.__name__
        self.assigning = False

    def add_task(self, task: Task):  # 添加任务
        pass

    def add_constrain(self, start_trigger: callable, run_mode: str = None):  # 添加约束
        # start_trigger 开始触发器
        # run_mode 作业运行模式
        pass

    def status(self):
        if self.assigning:
            return JobStatus.ASSIGNING

        for task in self.task_lst:
            if task.status == TaskStatus.WAITING:
                return JobStatus.WAITING
            elif task.status == TaskStatus.RUNNING:
                return JobStatus.RUNNING
        return JobStatus.FINISHED

    def available(self):  # 查看是否可以执行
        pass

    def __str__(self):
        job_str = '['
        for task in self.task_lst:
            job_str += str(task) + ';'
        job_str += ']'
        return job_str

    def serialize(self):
        pass
    @staticmethod
    def from_dict(dict):
        # 使用globals()函数获取类的引用
        job_class = globals()[dict['class_type']]
        c = job_class()
        for k, v in dict.items():
            value = v
            if k == 'task_lst':
                value = []
                # 字符串转列表
                for task_dict in v:
                    # 列表元素转task
                    task = Task.from_dict(task_dict)
                    value.append(task)
            setattr(c, k, value)
        return c


class ParallelTaskJob(Job):
    # 并行任务调度
    def __init__(self, max_process=-1):
        super().__init__(max_process)
        self.class_type = __class__.__name__

    def add_task(self, task: Task):
        self.task_lst.append(task)

    def add_constrain(self, start_trigger: callable = None, run_mode: str = 'all tasks'):
        # run_mode 作业运行模式。'all tasks'(所有任务全跑起来),'all devices'（所有卡全跑起来，每个卡跑一个任务，跑完再启动剩下的任务）
        self.start_trigger = start_trigger
        self.run_mode = run_mode

    def available(self):
        return self.start_trigger()

    def __str__(self):
        job_str = '['
        for task in self.task_lst:
            job_str += str(task) + ';'
        job_str += ']'
        return job_str

    def serialize(self):
        return {'task_lst':[task.serialize() for task in self.task_lst],'class_type':self.class_type}

class DependentTaskJob(Job):
    # 有依赖任务调度，彼此之间可能有依赖
    def __init__(self, max_process=-1):
        super().__init__(max_process)
        self.class_type = __class__.__name__

    def add_task(self, task: Task):
        self.task_lst.append(task)

    def add_constrain(self, start_trigger: callable, run_mode: str = None):
        self.start_trigger = start_trigger
        self.run_mode = run_mode

    def available(self):
        return self.start_trigger()
