package com.issel.cenote;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;

public class WriteTopology {
  public static void main(String[] args) {
    Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
    BrokerHosts hosts = new ZkHosts(dotenv.get("ZOOKEEPER_SERVERS", "83.212.104.172:2181,155.207.19.38:2181,83.212.96.15:2181"), "/brokers");
    String topicName = dotenv.get("KAFKA_TOPIC", "cenoteIncoming");
    SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
    kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 100;
    kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 100;
    kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 4);
    builder.setBolt("cockroach-bolt", new WriteToCockroach(), 64).shuffleGrouping("kafka-spout");
    Config config = new Config();
    config.setNumWorkers(16);
    config.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 60 * 60 * 2);
    try {
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
