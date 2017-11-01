#!/bin/sh
java -Dlogback.configurationFile=/root/gelf.xml -jar /root/hydra.jar --kafka=$Kafka_Broker --db=$Cassandra_Address --keyspace=$Cassandra_Keyspace --user=$Cassandra_Username --pass=$Cassandra_Password  --statsDHost=$StatsD_Host

