# import inspect
import inspect
from enum import Enum


class Task:
    num = 0

    def __init__(self, *args, **kwargs):
        Task.num = (Task.num + 1) % 1000
        self.tid = Task.num
        self.status = TaskStatus.WAITING
        self.class_type = __class__.__name__

    def __str__(self):
        return f'Task{self.tid}'

    def __call__(self):
        pass

    @staticmethod
    def from_dict(dict):
        # 使用globals()函数获取类的引用
        task_class = globals()[dict['class_type']]
        c = task_class(**dict)
        return c

class FunTask(Task):
    '''
    目前只能实现简单任务调用，环境没有配，假如引用别的自定义文件的函数也不行
    '''
    def __init__(self, fn_name:str , fn, param=None, **kwargs): #
        super().__init__()

        self.fn_name = fn_name
        if callable(fn):
            # self.fn = fn # 本地创建任务
            self.fn = inspect.getsource(fn)
        elif isinstance(fn,str):
            self.fn = fn# 服务器或客户端下载任务源代码
        self.param = param
        self.class_type = __class__.__name__

    def __str__(self):
        return self.fn_name + '_' + self.status.serialize()

    def __call__(self):
        exec_result = {}
        exec_code = f'{self.fn} \nresult={self.fn_name}({self.param})'
        exec(exec_code,{},exec_result) #TODO:传参
        return exec_result['result']

    def __getstate__(self):
        # 序列化
        return {'fn_name': self.fn_name, 'param': self.param, 'status': self.status,
                    'fn': self.fn, 'class_type': self.class_type,'tid':self.tid}

    def __setstate__(self, state):
        self.__dict__.update(state)

    def serialize(self):
        # 打包镜像
        # 考虑额外发送任务内容

        # 这里简单实现一下
        return {'fn_name': self.fn_name, 'param': self.param, 'status': self.status.serialize(),
                    'fn': self.fn, 'class_type':self.class_type,'tid':self.tid}

class TaskStatus(Enum):
    WAITING = "WAI"  # Waiting to start, Grey
    RUNNING = "RUN"  # Running, Blue
    FINISHED = "FIN"  # Finished, Green
    SYNCHING = "SYN"  # Synching, Cyan
    SUSPENDED = "SUS"  # Suspended, Yellow
    INTERRUPTED = "INT"  # Interrupted, Red

    def serialize(self):
        return self.value

    def __str__(self):
        return self.value
