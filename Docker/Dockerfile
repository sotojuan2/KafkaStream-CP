FROM openjdk:11-jdk-slim
ARG CONF=/test/config.txt
EXPOSE 9012
RUN apt-get update ; \
    apt-get install -y --no-install-recommends libjemalloc2 ; \
    apt-get install -y --no-install-recommends lsof ; \
    apt-get install -y --no-install-recommends procps ; \
    rm -rf /var/lib/apt/lists/*
ENV LD_PRELOAD=/usr/lib/aarch64-linux-gnu/libjemalloc.so.2
ENV ROCKSDB=/test/rocksdb.txt
#ENV LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2
COPY colour-1.0-SNAPSHOT.jar colour-1.0-SNAPSHOT.jar
#COPY otrasCosas.txt config.txt
#COPY otrasCosasOriginal.txt configOriginal.txt
CMD java -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.rmi.port=9012 -Dcom.sun.management.jmxremote.port=9012 -Djava.rmi.server.hostname=127.0.0.1 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Xmx64m -jar colour-1.0-SNAPSHOT.jar $CONF