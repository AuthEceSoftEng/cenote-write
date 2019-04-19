# cenote-write

> Apache Storm Topology used by cenote for data writing

[![Travis](https://img.shields.io/travis/com/AuthEceSoftEng/cenote-write.svg?style=flat-square&logo=travis&label=)](https://travis-ci.com/AuthEceSoftEng/cenote-write) [![license](https://img.shields.io/github/license/AuthEceSoftEng/cenote-write.svg?style=flat-square)](./LICENSE)

## Pipeline

* The `kafka-spout` Spout is connected to a kafka topic called `cenoteIncoming`.
* Everytime it consumes a message, it passes it down to `forwardToCassandra` Bolt.
* This bolt (which is written in Python) connects to a cassandra keyspace called `cenote`, and handles the data writing accoring to cenote's specs.

## Run to a cluster

* Compile the source code:

```bash
$ mvn clean install package
```

* Submit the topology to the cluster:

```bash
$ storm jar path/to/write-0.1.0-jar-with-dependencies.jar com.issel.cenote.WriteTopology WriteTopology
```