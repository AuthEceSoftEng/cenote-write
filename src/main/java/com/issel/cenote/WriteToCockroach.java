package com.issel.cenote;

import java.util.Map;

import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;

public class WriteToCockroach extends ShellBolt implements IRichBolt {
    static final long serialVersionUID = 1L;

    public WriteToCockroach() {
        super("/usr/bin/python3", "WriteToCockroach.py");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
