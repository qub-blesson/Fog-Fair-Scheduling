""" The Request Handler for Edge Fair Scheduler

This class handles all the client requests provided
the client has the correct access certificates. It also
serves as the main class for all of EFS and hence is responsible
for stating up all the sub-processes.

Arkadiusz Madej
"""


import struct
from Scheduler import Scheduler
from Monitor import Monitor
from threading import Thread
import socket
import ssl
import configparser
import sqlite3
import json
import psutil
import math

# Global variables
HOST = None
PORT = None
MAX_JOBS = None
MAX_QUEUE = None
BASE_CPU = None
BASE_MEM = None
CPU_UNIT = None
MEM_UNIT = None
MAX_CPU = None
PORT_RANGE_LOWER = None
PORT_RANGE_UPPER = None
STRATEGY = None

# SSL certificates
server_cert = 'certs/server.crt'
server_key = 'certs/server.key'
client_certs = 'certs/client.crt'


def read_config():
    """Reads the configuration file"""

    global HOST, PORT, MAX_QUEUE, BASE_CPU, BASE_MEM, CPU_UNIT, MEM_UNIT, MAX_CPU, PORT_RANGE_LOWER, PORT_RANGE_UPPER,\
        MAX_JOBS, STRATEGY

    # create config parses instance
    parser = configparser.ConfigParser()
    parser.read('config.ini')

    # read configuration file
    config = parser['SERVER']
    HOST = config['HOST']
    PORT = config.getint('PORT')
    MAX_QUEUE = config.getint('MAXQUEUE')
    PORT_RANGE_LOWER = config.getint('PORTLOWER')
    PORT_RANGE_UPPER = config.getint('PORTUPPER')
    MAX_CPU = config.getint('MAXCPU')
    BASE_CPU = config.getint('BASECPU')
    BASE_MEM = config.getint('BASEMEM')
    CPU_UNIT = config.getint('CPUUNIT')
    MEM_UNIT = config.getint('MEMUNIT')
    STRATEGY = config.getint('STRATEGY')

    # calculate max jobs allowed to run using the provided config and resources
    max_cpu = math.floor(((MAX_CPU * psutil.cpu_count()) - BASE_CPU) / CPU_UNIT)
    max_mem = math.floor(((psutil.virtual_memory().total / 1024 / 1024) - BASE_MEM) / MEM_UNIT)

    MAX_JOBS = min(max_cpu, max_mem)

    if STRATEGY not in range(0, 4):
        print("Bad configuration")
        exit(1)


def recv_message(sock):
    """Receives a message sent by the client

    Parameters:
        sock (socket): HTTP socket connection

    Returns:
        str: The message

    """

    # First acquire the message length
    msg_len = recv_data(sock, 4)

    if not msg_len:
        return None
    msg_len = struct.unpack('>I', msg_len)[0]

    # Return the full message, undecoded
    return recv_data(sock, msg_len)


def recv_data(sock, msg_len):
    """Receives data from HTTP connection

    Parameters:
        sock (socket): HTTP socket connection
        msg_len (int): Length of the message to receive

    Returns:
        str: The message

    """

    data = b''
    # Read the message data
    while len(data) < msg_len:
        packet = sock.recv(msg_len - len(data))
        if not packet:
            return None
        data += packet

    # Return undecoded message data
    return data


def send_msg(msg, conn):
    """Sends a structured message containing the message length at the start

    Parameters:
        msg (str): The message to be sent
        conn (socket): HTTP socket connection

    """

    msg = struct.pack('>I', len(msg)) + msg.encode('ascii')
    conn.sendall(msg)


def add_new_job(conn, addr, client, request):
    """If space is available in the queue, it adds a job otherwise informs client of rejection

    Parameters:
        conn (socket): HTTP socket connection
        addr (list): Client address structure
        client (str): Name of the client
        request (dict):  JSON dictionary containing the job request

    """

    # get size of job queue
    db = sqlite3.connect('edge.db')
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM job_queue")
    q_len = cur.fetchone()

    # if space available queue job else reject
    if q_len[0] <= MAX_QUEUE:
        cur.execute("INSERT INTO job_queue (cust_name, cust_ip, cust_port, priority, ports) VALUES (?, ?, ?, ?, ?)",
                    (client, addr[0], request['Job']['CommsPort'], request['Job']['Priority'], request['Job']['Ports']))
        db.commit()
        cur.execute("SELECT last_insert_rowid()")

        # get generated job ID
        job_id = cur.fetchone()[0]

        # notify client of job being accepted
        msg = {'Msg': 'Accepted', 'RequestType': 'Start', 'JobID': job_id}
        send_msg(json.dumps(msg), conn)
    else:
        # notify client of job being refused
        msg = {'Msg': 'Refused', 'Reason': 'No space in job queue'}
        send_msg(json.dumps(msg), conn)

    db.close()
    conn.close()


def terminate_job(conn, request):
    """Adds the job into the termination queue or removes from job queue if not yet started

    Parameters:
        conn (socket): HTTP socket connection
        request (dict): JSON dictionary containing the termination request

    """

    db = sqlite3.connect('edge.db')
    cur = db.cursor()

    job_id = request['JobID']

    # check if job is still in queue
    cur.execute("SELECT COUNT(*) FROM job_queue WHERE id=?", (job_id,))
    count = cur.fetchone()[0]

    # if job in queue then delete else queue for termination
    if count > 0:
        cur.execute("DELETE FROM job_queue WHERE id=?", (job_id,))
        # notify client of job being removed from queue
        msg = {'Msg': 'Terminated', 'JobId': job_id, 'Reason': 'Termination Requested'}
        send_msg(json.dumps(msg), conn)
    else:
        cur.execute("INSERT INTO term_queue(job_id, reason) VALUES (?,?)", (job_id, 'Termination Requested'))
        # notify client of job being queued for termination
        msg = {'Msg': 'Accepted', 'RequestType': 'Terminate', 'JobID': job_id}
        send_msg(json.dumps(msg), conn)

    conn.close()
    db.commit()
    db.close()


def handle_invalid_message(conn):
    """Used to inform the client of an invalid request

    Parameters:
        conn (socket): HTTP socket connection

    """

    msg = {'Msg': 'Refused', 'Reason': 'The request message was invalid'}
    send_msg(json.dumps(msg), conn)
    conn.close()


def get_peer_name(cert):
    """Extract the client name from an SSL certificate

    Parameters:
        cert (dict): SSL certificate

    Returns:
        str: Client name

    """

    subject = cert['subject']
    for x in subject:
        if x[0][0] == 'commonName':
            return x[0][1]


def handle_request(connection, addr, client):
    """Used to handle a newly received request

    Parameters:
        connection (socket): HTTP socket connection
        addr (list): Client address structure
        client (str): Name of the client

    """

    # read in received request as JSON
    request = json.loads(str(recv_message(connection), 'utf-8'))

    if request['Request'] == 'New Job':
        try:
            add_new_job(connection, addr, client, request)
        except sqlite3.DatabaseError:
            add_new_job(connection, addr, client, request)
    elif request['Request'] == 'Terminate':
        try:
            terminate_job(connection, request)
        except sqlite3.DatabaseError:
            terminate_job(connection, request)
    else:
        handle_invalid_message(connection)


def print_header():
    """Prints the header of EFS"""

    print('###############################################################')
    print('#               Fair Edge Job Scheduler Start                 #')
    print('###############################################################')
    print('')
    print('HOSTED ON: ', HOST)
    print('USING PORT: ', PORT)
    print('MAX QUEUE LENGTH: ', MAX_QUEUE)
    print('MAX JOBS: ', MAX_JOBS)
    print('')


def start_connection_service():
    """Starts the Request Handler"""

    print_header()

    # set up SSL
    SSL = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    SSL.verify_mode = ssl.CERT_REQUIRED  # to only allow authorised connections
    SSL.load_cert_chain(certfile=server_cert, keyfile=server_key)
    SSL.load_verify_locations(cafile=client_certs)

    #  set up socket to listen for incoming connections
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    connection.bind((HOST, PORT))
    connection.listen()

    print('Listening for incoming connections on {}:{}'.format(HOST, PORT))

    while True:
        try:
            # waits and accepts new HTTP connection
            new_conn, addr = connection.accept()
            print('New connection from {}:()'.format(addr[0], addr[1]))

            # wraps new connection with SSL for security
            ssl_conn = SSL.wrap_socket(new_conn, server_side=True)
            peer_cert = ssl_conn.getpeercert()
            client = get_peer_name(peer_cert)
            Thread(target=handle_request, args=(ssl_conn, addr,  client)).start()
        except KeyboardInterrupt:  # handles terminating EFS
            print('Shutting down fair edge job scheduler')
            if new_conn:
                new_conn.close()
            break


def start_scheduler_service():
    """Starts the Scheduler component"""

    sched = Scheduler(maxJobs=MAX_JOBS, unitCPU=CPU_UNIT, unitMem=MEM_UNIT, maxCPU=MAX_CPU, portLower=PORT_RANGE_LOWER,
                      portUpper=PORT_RANGE_UPPER, strategy=STRATEGY)
    sched.start()


def start_monitoring_service():
    """Starts the Monitor component"""

    monitor = Monitor()
    monitor.start()


def setup_db():
    """Sets up the database if it yet does not exist"""

    db = sqlite3.connect('edge.db')
    cur = db.cursor()

    # checks if tables exists in database, if not they are created
    cur.execute("CREATE TABLE if not exists jobs(id INTEGER PRIMARY KEY,cust_name TEXT NOT NULL,cust_ip TEXT NOT NULL,"
                "cust_port INTEGER,priority INTEGER,timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,ports TEXT NOT NULL);")
    cur.execute("CREATE TABLE if not exists job_queue(id INTEGER PRIMARY KEY AUTOINCREMENT,cust_name TEXT NOT NULL,"
                "cust_ip TEXT NOT NULL,cust_port INTEGER,priority INTEGER DEFAULT 1,"
                "timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,ports TEXT NOT NULL);")
    cur.execute("CREATE TABLE if not exists term_queue(job_id INTEGER PRIMARY KEY, reason TEXT,"
                " FOREIGN KEY(job_id) REFERENCES jobs(id));")

    # if tables were only just created it updates the sequence to start at 1000
    # required due to Docker not accepting value below for container ID
    cur.execute("SELECT seq FROM SQLITE_SEQUENCE WHERE name='job_queue'")
    if cur.fetchone() is None:
        cur.execute("INSERT INTO SQLITE_SEQUENCE(name,seq) VALUES('job_queue', 1000)")
    db.commit()
    db.close()


if __name__ == '__main__':
    setup_db()
    read_config()
    start_scheduler_service()
    start_monitoring_service()
    start_connection_service()
