import os

class Worker:
    def __init__(self,monitor=None):
        # 设置监控
        self.monitor = monitor

    def execute(self,param):
        # 执行任务
        pass

    def kill(self):
        # 任务停止执行
        pass
    def suspend(self):
        # 任务挂起
        pass