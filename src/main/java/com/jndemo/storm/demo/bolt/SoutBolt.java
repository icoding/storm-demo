package com.jndemo.storm.demo.bolt;


import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by jiangnan on 18/1/6.
 */
public class SoutBolt extends BaseBasicBolt {
    private TopologyContext context;

    /**
     * On create
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        this.context = context;
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String str = (String) tuple.getValue(0);
        System.out.println("out=" + str);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
