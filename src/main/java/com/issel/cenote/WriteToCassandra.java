package com.issel.cenote;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

public class WriteToCassandra extends ShellBolt implements IRichBolt {
    static final long serialVersionUID = 1L;

    public WriteToCassandra() {
        super("/usr/bin/python3", "WriteToCassandra.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
