import socket
import ssl
import struct
import json
from threading import Thread
import subprocess

host_addr = '192.168.0.50'  # replace with edge node IP
host_port = 6000  # replace with edge node port used for EFS
listening_port = 8000
server_sni_hostname = 'Edge'  # replace with edge node common name
server_cert = 'certs/server.crt'  # replace with edge node certificate path
client_cert = 'certs/arek.crt'  # replace with client certificate path
client_key = 'certs/arek.key'  # replace with client key path
ssh_path = '/root/.ssh/id_rsa.pub'  # replace with path to public ssh key


def recv_message(sock):
    # First acquire the message length
    msg_len = recv_data(sock, 4)
    if not msg_len:
        return None
    msg_len = struct.unpack('>I', msg_len)[0]
    # Return the full message, undecoded
    return recv_data(sock, msg_len)


def recv_data(sock, msg_len):
    # Read the message data
    data = b''
    while len(data) < msg_len:
        packet = sock.recv(msg_len - len(data))
        if not packet:
            return None
        data += packet
    # Return undecoded message data
    return data


def handle_conn(conn):
    message = json.loads(str(recv_message(conn), 'utf-8'))
    print(message)

    if message['Msg'] == 'Accepted':
        if message['RequestType'] == 'Start':
            print("Job accepted with ID {}".format(message['JobID']))
        else:
            print("Job termination accepted")
    elif message['Msg'] == 'Started':
        print("Job {} started with following port mappings {}".format(message['JobID'], message['Ports']))

        # read key as binary
        key_file = open(ssh_path, 'rb')
        key = key_file.read()
        print(key)
        key_file.close()

        # send rsa key
        conn.sendall(key)
    elif message['Msg'] == 'Terminated':
        print("Job {} terminated due to {}".format(message['JobID'], message['Reason']))
    elif message['Msg'] == 'Refused':
        print("Message refused because: {}".format(message['Reason']))

    conn.close()


def eternal_listener():
    print("Started with cert {} and key{}".format(client_cert, client_key))

    # set up SSL
    SSL = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    SSL.verify_mode = ssl.CERT_REQUIRED  # to only allow authorised connections
    SSL.load_cert_chain(certfile=client_cert, keyfile=client_key)
    SSL.load_verify_locations(cafile=server_cert)

    #  set up socket to listen for incoming connections
    connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    connection.bind(('0.0.0.0', listening_port))
    connection.listen()

    while True:
        conn, addr = connection.accept()
        ssl_conn = SSL.wrap_socket(conn, server_side=True)
        Thread(target=handle_conn, args=(ssl_conn,)).start()


def new_job(priority, ports):
    # set up SSL
    print("Using crt: {} and key: {}".format(client_cert, client_key))
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=server_cert)
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    # set up new SSL connection
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = context.wrap_socket(s, server_side=False, server_hostname=server_sni_hostname)
    conn.connect((host_addr, host_port))
    print("SSL connection established. Peer: {}".format(conn.getpeercert()))

    # form job request
    job = {'ID': 'None', 'Priority': priority, 'Ports': ports, 'CommsPort': listening_port}
    msg = {'Request': 'New Job', 'Job': job}

    # send request
    msg = json.dumps(msg)
    msg = struct.pack('>I', len(msg)) + msg.encode('ascii')
    conn.sendall(msg)

    handle_conn(conn)


def terminate_job(jobid):
    # set up SSL
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=server_cert)
    context.load_cert_chain(certfile=client_cert, keyfile=client_key)

    # set up new SSL connection
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn = context.wrap_socket(s, server_side=False, server_hostname=server_sni_hostname)
    conn.connect((host_addr, host_port))

    # form and send termination request
    msg = {'Request': 'Terminate', 'JobID': jobid}
    msg = json.dumps(msg)
    msg = struct.pack('>I', len(msg)) + msg.encode('ascii')
    conn.sendall(msg)

    handle_conn(conn)


def start():
    # runs continuously until user enters exit
    while True:
        try:
            print("Enter \"New Job\" for a new job request\n"
                  "Enter \"Terminate\" for a termination request\n"
                  "Or \"Exit\" to quit")
            option = input("What would you liked to do? Select from the available options above: ")
            if option.lower() == "new job":
                priority = int(input("Job Priority?"))
                ports = str(input("Enter required ports as list separated by commas (No Spaces)"))
                new_job(priority, ports)
                print("Start New Job")
            elif option.lower() == "terminate":
                jid = int(input("JobId?"))
                terminate_job(jid)
                print("Terminate Job")
            elif option.lower() == "exit":
                print("Bye Bye!")
                exit(0)
            else:
                print("INVALID OPTION SELECTED! TRY AGAIN!")
            subprocess.call('clear', shell=True)
        except KeyboardInterrupt:
            print("Bye Bye!")
            break


if __name__ == "__main__":
    Thread(target=eternal_listener).start()
    start()
