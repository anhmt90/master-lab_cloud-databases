FROM java:openjdk-8-jdk-alpine

ENV username ecs
ENV workdir /opt/app
ENV PATH "$PATH:$workdir"

RUN apk add --update openssh \
  && rm -rf /tmp/* /var/cache/apk/*
RUN addgroup -g 1000 -S $username \
  && adduser -u 1000 -s /bin/ash -D $username -G $username \
  && passwd -d $username

ADD docker-entrypoint.sh /usr/local/bin
ADD sshd_config /etc/ssh

RUN mkdir -p /opt/app
WORKDIR $workdir

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["/usr/sbin/sshd","-D"]
