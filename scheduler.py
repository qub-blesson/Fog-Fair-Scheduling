import psutil
import time
import math

# equal units of resources to be available to each job
equal_cpu = 10
equal_ram = 128*1024*1024  # 128MB in bytes
current_jobs = 0


# function to determine the maximum amount of jobs to run
def maximum_jobs(cpu, ram):
    max_cpu_jobs = cpu / equal_cpu
    max_ram_jobs = ram / equal_ram

    # the smallest value is returned because it's the maximum possible number of jobs to be run with
    # the available resources
    if max_cpu_jobs > max_ram_jobs:
        return math.trunc(max_ram_jobs)
    else:
        return math.trunc(max_cpu_jobs)


# function to start new job
def start():
    global current_jobs
    print('start')
    current_jobs += 1


# function to stop a job
def kill():
    global current_jobs
    print('kill')
    current_jobs -= 1


# function to decide whether a new job should be started or if one of the jobs needs stopped
def decide_scheduling(max_jobs):
    if max_jobs > current_jobs:
        start()
    else:
        kill()


# main function
while True:
    available_cpu = 100-psutil.cpu_percent()
    available_ram = psutil.virtual_memory().available

    # debug statements
    print('Available CPU: '+str(available_cpu)+' Available RAM: '+str(available_ram))
    print('Maximum jobs to be run are: '+str(maximum_jobs(available_cpu, available_ram))+' Currently running: '+str(current_jobs))

    decide_scheduling(maximum_jobs(available_cpu, available_ram))

    # to prevent spamming of CLI and easier debug
    time.sleep(1)
