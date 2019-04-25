FROM alpine
RUN apk update
RUN apk add openssh
RUN ssh-keygen -A
ADD ./sshd_config /etc/ssh/sshd_config
RUN echo "root:jdGsvthy2czt" | chpasswd
CMD /usr/sbin/sshd -D
