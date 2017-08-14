FROM openjdk:jdk

ENV SCALA_VERSION 2.12.3
ENV Kafka_Broker localhost:9092

WORKDIR /root
ADD target/scala-2.12/hydra.jar /root/hydra.jar
ADD entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/bash","/root/entrypoint.sh"]

