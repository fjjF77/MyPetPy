import logging
import time
from datetime import datetime
from Work.Manager import Manager
from Work.Job import ParallelTaskJob,DependentTaskJob
from Work.Task import Task,FunTask,TaskStatus

def fun(n:int):
    result = 1
    for i in range(n):
        result += 1
    return result

def sleep_fun(n:int):
    # run for n second, and print log each second.
    import time
    for i in range(n):
        print(f'{i}-th second.')
        time.sleep(1)

def check_time():
    if datetime.today().weekday()+1==2: # 周一
        return True
    else:
        return False

if __name__=='__main__':
    # 设置日志级别为DEBUG
    logging.basicConfig(level=logging.DEBUG)

    # exec_code = "def sleep_fun(n:int):\n\t# run for n second, and print log each second.\n\tfor i in range(n):\n\t\tprint(f\'{i}-th second.\')\n\t\ttime.sleep(1)\n" \
    #             "result=sleep_fun(2)"
    # print(exec_code)
    # exec(exec_code)

    manager = Manager() #max_process=2,run_mode='silent'

    result = None # 结果，如何设置会更好？
    job = ParallelTaskJob() # 创建任务
    task = FunTask(fn_name='sleep_fun',fn=sleep_fun,param=2)
    print(f'create a job, {job}.')
    job.add_task(task)
    # job.add_constrain(start_trigger=check_time) # 添加任务约束
    job.add_constrain(run_mode='all tasks') #'all devices'
    print(f'release a job, {job}.')
    manager.release(job=job) # 发布任务
    # Manager.release_work(work=fun,args=) #命令行形式

    try:
        manager.start()  # 监听任务仓库与本地资源，持续执行
        print(f'manager start.')
        # job.start() # 任务开始
        # job.suspend() # 任务挂起
        # job.recovery() # 任务恢复
    except Exception as e:
        raise e

    if task.status == TaskStatus.FINISHED:  # TODO:获取task结果，异步通知
        print(f'task {task.tid} has completed, it\'s result is {task.result}')

