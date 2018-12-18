# cenote-write

> Apache Storm Topology used by cenote for data writing

## Pipeline

* The `kafka-spout` Spout is connected to a kafka topic called `cenoteIncoming`.
* Everytime it consumes a message, it passes it down to `forwardToCassandra` Bolt.
* This bolt (which is written in Python) connects to a cassandra keyspace called `cenote`, and handles the data writing accoring to cenote's specs.

## Run to a cluster

* Compile the source code:

```bash
$ mvn clean package
```

* Submit the topology to the cluster:

```bash
$ storm jar path/to/write-0.1.0-jar-with-dependencies.jar com.issel.cenote.WriteTopology WriteTopology
```