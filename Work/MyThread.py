import os
import threading

from .Task import Task,TaskStatus

class MyThread(threading.Thread):
    def __init__(self, task:Task):
        super().__init__()
        self.task = task

    def run(self):
        self.result = self.task()
        self.task.status = TaskStatus.FINISHED

    def get_result(self):
        try:
            return self.result
        except Exception:
            return None