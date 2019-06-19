package com.issel.cenote;

import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

public class WriteTopology {
  public static void main(String[] args) {
    Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();
    String topicName = dotenv.get("KAFKA_TOPIC", "cenoteIncoming");
    String bootstrapServers = dotenv.get("KAFKA_SERVERS", "155.207.19.241:9092,155.207.19.234:9092,155.207.19.237:9092");

    KafkaSpoutConfig<String, String> kafkaSpoutConfig = KafkaSpoutConfig
            .builder(bootstrapServers, topicName)
            .setProp("group.id", "/" + topicName)
            .setProp(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 1024 * 1024 * 100)
            .setProp(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 1024 * 1024 * 100)
            .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
            .build();

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", new KafkaSpout<>(kafkaSpoutConfig), 4);
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
