package com.issel.cenote;

import java.util.UUID;

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
        Config config = new Config();
        config.setDebug(true);
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        BrokerHosts hosts = new ZkHosts("83.212.104.172:2181,83.212.104.177:2181,83.212.96.15:2181", "/brokers");
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, "cenoteIncoming", "/brokers/topics/" + "cenoteIncoming", UUID.randomUUID().toString());
        kafkaSpoutConfig.bufferSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.fetchSizeBytes = 1024 * 1024 * 4;
        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig), 8);
        builder.setBolt("forwardToCassandra", new WriteToCassandra(), 8).shuffleGrouping("kafka-spout");
        config.setNumWorkers(5);
        try {
            StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
