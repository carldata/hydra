# Hydra

[![Build status](https://travis-ci.org/carldata/hydra.svg?branch=master)](https://travis-ci.org/carldata/hydra)

Stream processing engine which executes custom script written in [FlowScript](http://github.com/carldata/flow-script).

Interface to this engine is based on Kafka
 
## Quick start

### Run Kafka


Run Zookeeper and Kafka and create topics
```bash
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Prepare Kafka. This should be run only once
```bash
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic data
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic hydra-rt
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic hydra-batch
bin/kafka-topics.sh --zookeeper localhost:2181 --list
```

 ```bash
sbt assembly
java -jar target/scala-2.12/hydra.jar 
 ```

Run consumer to listen to the topic:

```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic data
```

### Feed some data

Use [Theia](https://github.com/carldata/theia) as a data generator.


### Datadog metric type


| Matric name | Description |
|-------------|-------------|
|`hydra.batch` | Number of processed batch events |
|`hydra.batch.in.records` | Number of records read from database by batch processing |
|`hydra.batch.out.records` | Number of generated records by batch processing |
|`hydra.batch.rate` | Rate for batch processing |
|`hydra.batch.errors.parser` | Number of failed to deserialize batch processing jobs |
|`hydra.rt.count` | Number of processed real-time jobs |
|`hydra.rt.out.count` | Number of records generated by real-time processing job |
|`hydra.rt.errors.parser` | number of records failed to deserialize by real-time processing job |


# Join in!

We are happy to receive bug reports, fixes, documentation enhancements,
and other improvements.

Please report bugs via the
[github issue tracker](http://github.com/carldata/hydra/issues).



# Redistributing

Hydra source code is distributed under the Apache-2.0 license.

**Contributions**

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
licensed as above, without any additional terms or conditions.
