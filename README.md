# EFS - An Edge Fair Scheduler 

Edge computing aims to bring data processing closer to the edge of the network. By hosting services which usually reside in the Cloud closer to the end user, it aims to reduce the communication latencies. Fair scheduling of requests for offloading those services onto Edge nodes is an important challenge. EFS aims to provide a prototype for managing the Edge resources and fairly distributing them between the requested services whilst maintaining the base service of the Edge node. 

This is a developing research project and some features might not be stable yet.

# License
All source code, documentation, and related artifacts associated with the EFS open source project are licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

# How to Use
In order to run EFS please follow the steps below:
1. Make the install script executable
    ```bash
    chmod +x install.sh
    ```
    
2. Run the install script
    ```bash
    ./install.sh
    ```
    
    Linux might complain that there are Windows characters present in the script. In this case run:
    ```bash
    sed -i -e 's/\r$//' install.sh; ./install.sh
    ```
    
3. Configure the config.ini within /root/EFS as you desire. Default provided config explained below:
    ```bash
    [SERVER]
    host = 0.0.0.0
    port = 6000
    maxqueue = 100000
    basecpu = 100000
    basemem = 256
    maxcpu = 100000
    cpuunit = 50000
    memunit = 256
    portlower = 10000
    portupper = 19999
    strategy = 0
    ```
    - **host** – The IP address to bind the socket to. Leave as 0.0.0.0 to bind to all edge node addresses\
    - **port** – The port number used for EFS communication\
    - **maxqueue** – The maximum number of jobs allowed to be in the queue\
    - **basecpu** – The amount of CPU required by the base service. One core is equal to the value set in 
                 maxcpu\
    - **basemem** – The amount of RAM memory required by the base service in megabytes\
    - **maxcpu** – The CPU scheduler period, best left default as 100 microseconds (100000)\
    - **cpuunit** – The value of a single CPU unit which will be assigned to each Docker containers. In this 
                the value corresponds to half a core since 50000/100000 = 0.5\
    - **memunit**– The value of a single RAM memory unit which will be assigned to each Docker
                  container in megabytes\
    - **portlower** – Denotes the start of the range of ports which can be used for the containers\
    - **portupper** – Denotes the last value of the range of ports which can be used for the containers\
    
4. Generate the server certificate
    ```bash
    openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout server.key -out server.crt
    ```
    You will be asked to fill in a series of fields which are mainly self explanatory. However it is important that you specify a **Common Name** you will remember as this is required by the clients
    
5. Move the generated key and certificate to the certificates' directory
    ```bash
    mv servey.key server.crt /root/EFS/certs/
    ```

6.  Now you need to generate the client certificates. Replace <client> with the client name
    ```bash
    openssl req -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout <client>.key -out <client>.crt
    ```
    Once again you will have to fill in a series of fields. The **Common Name** here is important as it's used to indentify the client and should match the naming used for the certificate and key
    
7. Move the generated key and certificate to the certificates' directory
    ```bash
    mv <client>.key <client>.crt /root/EFS/certs/
    ``` 

8. Now append the new client key to the client keys file
    ```bash
    cat /root/EFS/certs/<client>.key >> /root/EFS/certs/client.crt
    ```
    
9. Distribute the client key and client and server certificate to the client using your preferred method
10. You can now start EFS
    ```bash
    cd /root/EFS/; python3.5 EFS.py
    ```
    