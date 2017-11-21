#!/bin/sh
java -Dlogback.configurationFile=/root/gelf.xml -jar /root/hydra.jar --kafka=$KAFKA_BROKER --db=$CASSANDRA_ADDRESS --keyspace=$CASSANDRA_KEYSPACE --user=$CASSANDRA_USERNAME --pass=$CASSANDRA_PASSWORD  --statsd-host=$STATSD_HOST

