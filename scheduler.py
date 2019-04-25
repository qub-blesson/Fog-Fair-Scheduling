""" The Scheduler for Edge Fair Scheduler

This is the scheduler class which is responsible for
the scheduling of all the jobs on the edge node.

Arkadiusz Madej
"""

import random
import threading
import ssl
import psutil
import docker
import socket
import sqlite3
import json
import struct
from tarfile import TarFile
from threading import Thread


class Scheduler(Thread):

    def __init__(self, maxJobs, unitCPU, unitMem, maxCPU, portUpper, portLower, strategy):
        """Variable initialisation for the class"""
        super(Scheduler, self).__init__()
        self.stopRequest = threading.Event()
        self.strategy = strategy

        self.maxCPU = maxCPU  # per core
        self.unitCPU = unitCPU
        self.maxJobs = maxJobs
        self.unitMem = unitMem

        self.dockr = docker.from_env()
        self.db = None
        self.db_cur = None
        self.ports_lower = portLower
        self.ports_upper = portUpper

        # SSL certificates
        self.server_cert = 'certs/server.crt'
        self.server_key = 'certs/server.key'
        self.client_cert = None

    def send_msg(self, msg, conn):
        """Sends a structured message containing the message length at the start

        Parameters:
            msg (str):  The message to be sent
            conn (socket): HTTP socket connection

        """

        msg = struct.pack('>I', len(msg)) + msg.encode('ascii')
        conn.sendall(msg)

    def check_resource(self):
        """Checks if there are resources available for a next job

        Returns:
            bool: True/False whether resources available

        """

        # get amount of CPU and RAM available
        availableCPU = 100 - psutil.cpu_percent()
        availableMem = psutil.virtual_memory().available/1024/1024

        # if available resources are at least equal to the CPU and RAM unit of a job return True
        if availableCPU >= self.unitCPU / (self.maxCPU * psutil.cpu_count()) and availableMem >= self.unitMem:
            return True
        return False

    def stop_all_containers(self):
        """Stops all of the running containers when EFS is shutting down"""

        while len(self.dockr.containers.list()) > 0:
            container = self.dockr.containers.list()[0]
            container.stop()

    def recv_key(self, conn):
        """Receives an SSH key

        Parameters:
            conn (socket): HTTP socket connection

        Returns:
            str: SSH key string

        """
        # Read the message data
        data = b''
        while True:
            part = conn.recv(1024)
            if not part:
                break
            data += part

        return data

    def notify_client(self, conn, job_id, ports):
        """Notifies the client about the job being started

        Parameters:
            conn (socket): HTTP scoket connection
            job_id (int): The ID of the job which was started
            ports (dict): Dictionary of the port mappings

        """

        msg_dict = {'Msg': 'Started', 'JobID': job_id, 'Ports': ports}
        self.send_msg(json.dumps(msg_dict), conn)

    def get_ssh_key(self, conn):
        """Receives and saves an SSH key

        Parameters:
            conn (socket): HTTP scoket connection

        """

        # open file for writing
        key_file = open('id_rsa.pub', 'wb')

        # receive key
        key = self.recv_key(conn)

        # write key to file and close
        key_file.write(key)
        key_file.close()

    def get_queue_size(self):
        """Gets the size of the job queue

        Returns:
            int: Size of the queue

        """

        self.db_cur.execute("SELECT COUNT(*) FROM job_queue")
        size = self.db_cur.fetchone()[0]
        return int(size)

    def get_next_client(self, priority=False):
        """ Selects the next client whose job should be scheduled

        Parameters:
            priority (bool): Boolean whether  priority needs to be considered
                (default is False)

        Returns:
            str: The client name
            (optional) int: The priority

        """

        client_freq = {}
        next_priority = 3

        # if priority needs to be considered then get the next priority to schedule
        # retrieve a list of waiting client
        if priority:
            next_priority = self.get_next_priority()
            self.db_cur.execute("SELECT cust_name from job_queue WHERE priority=?", (next_priority,))
        else:
            self.db_cur.execute("SELECT cust_name from job_queue")

        waiting_clients = set([result[0] for result in self.db_cur.fetchall()])

        # for each waiting client get the previously run jobs frequency
        for c in waiting_clients:
            if priority:
                self.db_cur.execute("SELECT COUNT(*) FROM jobs WHERE timestamp>=date('now','-7 day') AND cust_name=? "
                                    "AND priority=?", (c, next_priority))
            else:
                self.db_cur.execute("SELECT COUNT(*) FROM jobs WHERE timestamp>=date('now','-7 day') AND cust_name=?",
                                    (c,))
            count = self.db_cur.fetchone()[0]
            client_freq[c] = count

        # returns the next client whose job needs scheduled
        # if priority to be considered als returns the priority
        if priority:
            return min(client_freq, key=client_freq.get), next_priority
        else:
            return min(client_freq, key=client_freq.get)

    def get_next_priority(self):
        """Selects the next job priority to be scheduled

        Returns:
            int: The job priority

        """

        priority_weighted = {3: 0.5, 2: 0.35, 1: 0.15}
        priority_freq = {}

        #  get total number of previously run jobs
        self.db_cur.execute("SELECT COUNT(*) FROM jobs WHERE timestamp>=date('now','-7 day')")
        total_freq = self.db_cur.fetchone()[0]

        # grab all waiting priorities
        self.db_cur.execute("SELECT priority from job_queue")
        waiting_priorities = set([result[0] for result in self.db_cur.fetchall()])

        # calculate frequency of job execution per each waiting priority
        for p in waiting_priorities:
            self.db_cur.execute("SELECT COUNT(*) FROM jobs WHERE timestamp>=date('now','-7 day') AND priority=?", (p,))
            count = self.db_cur.fetchone()[0]

            if count > 0 and total_freq > 0:
                priority_freq[p] = count/total_freq
            else:
                priority_freq[p] = 0.0

        # sort waiting priorities from highest to lowest
        ordered = sorted(waiting_priorities, reverse=True)

        return self.select_priority(ordered, priority_freq, priority_weighted)

    def select_priority(self, waiting, priority_freq, priority_weighted, index=0):
        """ Selects the next job priority to be scheduled from the waiting list
        based on the priority weightings

        Parameters:
            waiting (list): List of priorities in the job queue
            priority_freq (dict): Dictionary of priority frequencies
            priority_weighted (dict): Dictionary of priority weightings
            index (int): The index value at which to start looking through the waiting list
                (default is 0)

        Returns:
            int: The job priority

        """

        if priority_freq[waiting[index]] < priority_weighted[waiting[index]]:
            return waiting[index]  # if priority under threshold then return it
        elif index == len(waiting) - 1:
            return waiting[0]  # get highest priority in the case all are over their threshold
        else:
            return self.select_priority(waiting, priority_freq, priority_weighted, index + 1)

    def get_next_job(self):
        """Selects the next job based on the time of request

        Returns:
            list: The next job to run

        """

        # gets oldest job first
        self.db_cur.execute("SELECT * FROM job_queue ORDER BY datetime(timestamp) ASC LIMIT 1")
        job = self.db_cur.fetchone()
        job_id = job[0]

        # move job record to jobs history table
        self.db_cur.execute("INSERT INTO jobs SELECT * FROM job_queue WHERE id=?", (job_id,))
        self.db_cur.execute("DELETE FROM job_queue WHERE id=?", (job_id,))
        self.db.commit()
        return job

    def get_next_job_clients(self):
        """Selects the next job based on client frequency

        Returns:
            list: The next job to run

        """

        # get the next client whose job to schedule
        next_client = self.get_next_client()

        # get oldest job entry for the client
        self.db_cur.execute("SELECT * FROM job_queue WHERE cust_name=? ORDER BY datetime(timestamp) ASC LIMIT 1",
                            (next_client, ))
        job = self.db_cur.fetchone()
        job_id = job[0]

        # move job record to jobs history table
        self.db_cur.execute("INSERT INTO jobs SELECT * FROM job_queue WHERE id=?", (job_id,))
        self.db_cur.execute("DELETE FROM job_queue WHERE id=?", (job_id,))
        self.db.commit()
        return job

    def get_next_job_priority(self):
        """Selects the next job based on job priority

        Returns:
            list: The next job to run

        """

        # get next job priority to schedule
        next_priority = self.get_next_priority()

        # gets oldest job entry with specified priority
        self.db_cur.execute("SELECT * FROM job_queue WHERE priority=? ORDER BY datetime(timestamp) ASC LIMIT 1",
                            (next_priority,))
        job = self.db_cur.fetchone()
        job_id = job[0]

        # move job record to jobs history table
        self.db_cur.execute("INSERT INTO jobs SELECT * FROM job_queue WHERE id=?", (job_id,))
        self.db_cur.execute("DELETE FROM job_queue WHERE id=?", (job_id,))
        self.db.commit()
        return job

    def get_next_job_priority_client(self):
        """Selects the next job based on job priority and client frequency

        Returns:
            list: The next job to run

        """

        # get the next priority and client whose job to schedule next
        next_client, next_priority = self.get_next_client(priority=True)

        # gets oldest job entry for specified client and priority
        self.db_cur.execute("SELECT * FROM job_queue WHERE cust_name=? AND priority=? ORDER BY datetime(timestamp) "
                            "ASC LIMIT 1", (next_client, next_priority))
        job = self.db_cur.fetchone()
        job_id = job[0]

        # move job record to jobs history table
        self.db_cur.execute("INSERT INTO jobs SELECT * FROM job_queue WHERE id=?", (job_id,))
        self.db_cur.execute("DELETE FROM job_queue WHERE id=?", (job_id,))
        self.db.commit()
        return job

    def setup_ssh(self, container):
        """Sets up passwordless access to the specified container

        Parameters:
             container (Container): ID of the container to set up access for

        """

        # create tarball with ssh public key
        with TarFile(name='key.tar', mode='w') as t:
            t.add('id_rsa.pub')

        # read in tar file as binary
        tar = open('key.tar', mode='rb')
        data = tar.read()

        # put public key into temp folder of container
        container.put_archive('/tmp', data)
        container.exec_run('mkdir -p /root/.ssh/')
        container.exec_run('scp /tmp/id_rsa.pub /root/.ssh/authorized_keys')
        print('ssh setup')

    def get_free_ports(self, num):
        """Finds the required number of free ports

        Parameters:
            num (int): Number of required free ports

        Returns:
            list: A list of free  ports

        """

        ports = []

        # get list of used ports
        used_ports = self.get_used_ports()
        while len(ports) < num:
            # generate random port
            port = random.randint(self.ports_lower, self.ports_upper)
            #  if port not being used then append to free ports list
            if port not in used_ports:
                ports.append(port)

        return ports

    def get_used_ports(self):
        """Gathers the ports currently being used by the running containers

        Returns:
              list: A list of used ports

        """

        # get list of all running containers
        containers = self.dockr.containers.list()
        client = docker.APIClient(base_url='unix://var/run/docker.sock')

        used_ports = []
        for c in containers:

            # gather the used ports by each of the containers
            ports = client.inspect_container(c.id)['HostConfig']['PortBindings']
            for port in ports:
                used_ports.append(ports[port][0]['HostPort'])

        return used_ports

    def map_ports(self, ports):
        """Given a list of ports maps them to available ports on edge node

        Parameters:
            ports (str): List of required ports separated by commas

        Returns:
            dict: A dictionary of mapped ports

        """

        mapped_ports = {}

        # create list of required ports from the ports string
        req_ports = ports.split(',')
        req_ports.append('22')  # to allow ssh access

        # get free ports
        free_ports = self.get_free_ports(len(req_ports))

        # map required ports to free ports
        for i in range(len(req_ports)):
            mapped_ports[req_ports[i]] = free_ports[i]

        return mapped_ports

    def start_container(self, job_id, ports):
        """Used to start a container with the correct ID and port mapping

        Parameters:
            job_id (int): The job ID to use as the container ID
            ports (dict): Dictionary of the mapped ports

        Returns:
            Container/None: If successful a Docker container else None
        """

        try:
            return self.dockr.containers.run("arek/alpine_ssh", cpu_period=self.maxCPU, tty=True, cpu_quota=self.unitCPU,
                                             mem_limit=self.unitMem * 1024 * 1024, detach=True, name=str(job_id),
                                             network_mode='bridge', ports=ports)
        except docker.errors.APIError:
            return None

    def start_job(self):
        """Called by the main function to start a new job"""

        # call appropriate method based on what's specified in config
        if self.strategy == 0:
            job = self.get_next_job()
        elif self.strategy == 1:
            job = self.get_next_job_clients()
        elif self.strategy == 2:
            job = self.get_next_job_priority()
        else:
            job = self.get_next_job_priority_client()

        # get dictionary of mapped ports
        ports_dict = self.map_ports(job[6])

        # set up secure communication with client using SSL
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile='certs/'+job[1]+'.crt')
        context.load_cert_chain(certfile=self.server_cert, keyfile=self.server_key)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn = context.wrap_socket(s, server_side=False, server_hostname=job[1])
        conn.connect((job[2], job[3]))

        print("Start container {}".format(job[0]))

        # start container
        try:
            container = self.start_container(job[0], ports_dict)
        except docker.errors.APIError:
            self.dockr = docker.from_env()
            container = self.start_container(job[0], ports_dict)

        if container is None:
            # sometimes a port conflict error occurs
            ports_dict = self.map_ports(job[6])
            container = self.start_container(job[0], ports_dict)

        # if container started successfully notify client and set up SSH
        if container is not None:
            print('about to notify {}:{}'.format(job[2], job[3]))
            self.notify_client(conn, job[0], ports_dict)
            self.get_ssh_key(conn)
            self.setup_ssh(container)
        else:
            print("Unable to start the job")

    def run(self):
        """Main function responsible for the scheduling of jobs"""

        # initialise database connection
        self.db = sqlite3.connect('edge.db')
        self.db_cur = self.db.cursor()

        print('Scheduler Initialised')

        while not self.stopRequest.is_set():
            currentJobs = len(self.dockr.containers.list())
            if self.get_queue_size() > 0 and currentJobs < self.maxJobs:  # check if more jobs allowed
                if self.check_resource():  # check if resources available
                    self.start_job()

    def join(self, timeout=None):
        """Called when the EFS is being shut down, stopping the Thread safely"""
        self.stopRequest.set()
        self.stop_all_containers()

        # delete all unused containers
        self.dockr.containers.prune()
        super(Scheduler, self).join(timeout)
