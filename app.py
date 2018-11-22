from flask import Flask, request
from flask import jsonify
import psutil
import docker
import queue

priority1 = queue.Queue()
priority2 = queue.Queue()
priority3 = queue.Queue()

availableCPU = 0
occupiedCPU = 0
availableRAM = 0
client = docker.DockerClient(base_url='tcp://localhost:2375')

alljobs = [priority1, priority2, priority3]

app = Flask(__name__)


# Method to get the currently available resources
def get_resources():
    global availableCPU, availableRAM
    availableCPU = psutil.cpu_count() - occupiedCPU
    availableRAM = psutil.virtual_memory().available


# Temporary method to view number of queued jobs
@app.route('/job/all')
def get_jobs():
    jobs = alljobs[0].qsize()
    jobs += alljobs[1].qsize()
    jobs += alljobs[2].qsize()
    return str(jobs)


# Method to queue job based on priority
def queue_job(jobid, priority, cpu, ram):
    job = {'jobId': jobid, 'priority': priority, 'CPU': cpu, 'RAM': ram}
    if priority == 1:
        priority1.put(job)
    elif priority == 2:
        priority2.put(job)
    elif priority == 3:
        priority3.put(job)


# Method to stop a job
def terminate_job(container_id):
    container = client.containers.get(container_id)
    container.stop()


# Method used to determine whether to run or queue the job
def decide(jobid, priority, cpu, ram):
    global occupiedCPU
    get_resources()
    if cpu <= availableCPU and ram <= availableRAM:
        print("Starting new job")

        # Currently job is static to use 1 cpu core
        client.containers.run('progrium/stress', command='--cpu 1', detach=True)
        occupiedCPU += cpu
        return 'Job ran on edge node'
    else:
        print("Queueing job")
        queue_job(jobid, priority, cpu, ram)
        return 'Job queued on edge node'


# Method to handle a job request coming in
@app.route('/job/', methods=['PUT'])
def job_request():

    # Reads in all arguments from request
    jobid = int(request.args['jobid'])
    priority = int(request.args['priority'])
    cpu = int(request.args['cpu'])
    ram = int(request.args['ram'])

    # Gets response from function, which determines if a job is run or queued, to respond to request
    response = decide(jobid, priority, cpu, ram)
    return response


if __name__ == '__main__':
    app.run(port='6000')


