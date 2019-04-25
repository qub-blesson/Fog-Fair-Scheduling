""" The Monitor for Edge Fair Scheduler

This class is responsible for monitoring the resource usage of all
running containers and terminating any idle ones. It also terminates
any jobs which have been requested for termination by the clients.

Arkadiusz Madej
"""

import docker
import time
import datetime
import threading
import sqlite3
import psutil
import ssl
import socket
import struct
import json


class Monitor(threading.Thread):

    def __init__(self):
        """Variable initialisation for the class"""

        super(Monitor, self).__init__()
        self.stopRequest = threading.Event()
        self.dockr = docker.from_env()
        self.dockr_client = docker.APIClient(base_url='unix://var/run/docker.sock')
        self.db = None
        self.db_cur = None

        self.server_cert = 'certs/server.crt'
        self.server_key = 'certs/server.key'
        self.client_cert = None

    def queue_for_termination(self, containers):
        """Given a list of idle containers, it queues all of them for termination

        Parameters:
            containers (list): List of idle containers

        """

        for container in containers:
            self.db_cur.execute("INSERT INTO term_queue (job_id,  reason) VALUES (?,?)", (container, 'Container Idle'))
            print("Kill job {}".format(container))
        self.db.commit()

    def terminate_jobs(self):
        """Called periodically in order to stop any containers listed in the termination queue"""

        # gets all termination request from queue
        self.db_cur.execute("SELECT * FROM term_queue")
        containers = self.db_cur.fetchall()

        # if queue not empty start terminating
        if len(containers) > 0:
            for c in containers:
                container = None
                try:
                    print("Stopping {}".format(c[0]))
                    container = self.dockr.containers.get(str(c[0]))
                except docker.errors.APIError:
                    self.dockr = docker.from_env()
                    print("Job is already stopped or never existed")

                # stop and remove container
                if container is not None:
                    container.stop()
                    container.remove(v=True)

                # once job terminate remove from queue and notify client
                self.db_cur.execute("DELETE FROM term_queue WHERE job_id=?", (c[0],))
                self.db.commit()
                if container is not None:
                    self.notify_client(c[0], c[1])

    def notify_client(self, id, reason):
        """Used to notify the client that a container has been stopped

        Parameters:
            id (int): Client ID
            reason (str): Reason for terminating job

        """

        # get client information based on job ID
        self.db_cur.execute("SELECT * FROM jobs WHERE id=?", (id,))
        job = self.db_cur.fetchone()

        # set up secure communication with client using SSL
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile='certs/'+job[1]+'.crt')
        context.load_cert_chain(certfile=self.server_cert, keyfile=self.server_key)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn = context.wrap_socket(s, server_side=False, server_hostname=job[1])
        conn.connect((job[2], job[3]))

        # send notification to client
        msg_dict = {'Msg': 'Terminated', 'JobID': id, 'Reason': reason}
        self.send_msg(json.dumps(msg_dict), conn)
        conn.close()

    def send_msg(self, msg, conn):
        """Sends a structured message containing the message length at the start

        Parameters:
            msg (str): The message to be sent
            conn (socket): HTTP socket connection

        """

        msg = struct.pack('>I', len(msg)) + msg.encode('ascii')
        conn.sendall(msg)

    def get_cpu_stats(self):
        """Collects the CPU statistics for all containers running for over a minute

        Returns:
              dict: A dictionary of the collected CPU statistics

        """

        jobs = {}
        for c in self.dockr_client.containers():
            times = {}

            # calculate how long container has been running for
            start = c['Created']
            uptime = datetime.datetime.now() - datetime.datetime.fromtimestamp(start)

            if uptime.total_seconds() > 60:  # only check jobs running for over a minute

                # get CPU statistics for container
                container = self.dockr.containers.get(c['Id'])
                stats = container.stats(stream=False)
                total = float(stats['cpu_stats']['cpu_usage']['total_usage'])
                system = float(stats['cpu_stats']['system_cpu_usage'])

                # add statistics to dictionary and then append it to jobs list
                times['total'] = total
                times['system'] = system
                jobs[int(container.name)] = times
        return jobs

    def calculate_percentages(self, current, previous):
        """Given a set of CPU statistics it calculates the average CPU usage percentages
        per container

        Parameters:
            current (dict): Dictionary of current CPU statistics
            previous (dict): Dictionary of previous CPU statistics

        Returns:
              dict: A dictionary of CPU usage percentages

        """

        percentages = {}
        for i in current:
            if i in previous:  # only compare stats for containers which exist in both statistics lists
                total_delta = current[i]['total'] - previous[i]['total']
                system_delta = current[i]['system'] - previous[i]['system']

                # calculate the average CPU usage percentage
                if total_delta > 0.0 and system_delta > 0.0:
                    percentages[i] = (total_delta/system_delta) * 100.0 * psutil.cpu_count()
                else:
                    percentages[i] = 0.0
            else:
                percentages[i] = 100.0
        return percentages

    def check_for_idle_containers(self, current, previous):
        """Checks if any of the running containers are idle

        Parameters:
            current (dict): Dictionary of current CPU statistics
            previous (dict): Dictionary of previous CPU statistics

        Returns:
            list: A list of idle containers

        """

        percentages = self.calculate_percentages(current, previous)
        containers = []
        for i in percentages:
            if percentages[i] < 10.0:  # if container uses less than 10% CPU queue for termination
                containers.append(i)
        return containers

    def run(self):
        """Main function responsible for the monitoring and termination of containers"""

        # initiate db connection
        self.db = sqlite3.connect('edge.db')
        self.db_cur = self.db.cursor()

        timeout = datetime.datetime.now()

        while not self.stopRequest.is_set():
            if datetime.datetime.now() >= timeout:
                # gather CPU statistics for a 10 second period
                try:
                    previous = self.get_cpu_stats()
                    time.sleep(10)
                    current = self.get_cpu_stats()

                    # using gathered stats check for idle containers
                    idle = self.check_for_idle_containers(current, previous)
                except docker.errors.APIError:
                    # restart API connection if it breaks
                    self.dockr = docker.from_env()

                # queue any idle containers
                if len(idle) > 0:
                    self.queue_for_termination(idle)

                # sleep for 2 minutes before checking again
                timeout = datetime.datetime.now() + datetime.timedelta(minutes=2)

            self.terminate_jobs()

            # prevent overloading cpu when no jobs to terminate
            time.sleep(1)

    def join(self, timeout=None):
        """Called when the EFS is being shut down, stopping the Thread safely"""

        self.stopRequest.set()
        super(Monitor, self).join(timeout)
