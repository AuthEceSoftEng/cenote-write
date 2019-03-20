package com.issel.cenote;

import java.util.UUID;

import io.github.cdimascio.dotenv.Dotenv;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

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
    builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 3).setNumTasks(6);
    builder.setBolt("cockroach-bolt", new WriteToCockroach(), 3).setNumTasks(6).shuffleGrouping("kafka-spout");
    Config config = new Config();
    config.setNumWorkers(12);
    config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 50000);
    try {
      StormSubmitter.submitTopology(args[0], config, builder.createTopology());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
