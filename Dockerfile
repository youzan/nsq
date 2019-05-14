FROM busybox

ADD dist/docker/bin/ /nsq_bin/
ADD shell /nsq_bin/
RUN cd /    && ln -s /nsq_bin/* . \
 && cd /bin && ln -s /nsq_bin/* . \
 && chmod 777 start-nsqd.sh \
 && chmod 777 start-nsqlookupd.sh

EXPOSE 4150 4151 4160 4161 4170 4171

VOLUME /data
VOLUME /etc/ssl/certs
