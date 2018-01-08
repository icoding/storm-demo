package com.jndemo.storm.demo.bolt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.mapper.TupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.kafka.bolt.selector.KafkaTopicSelector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;
import java.util.Properties;

/**
 * Created by jiangnan on 18/1/6.
 */
public class KafkaJnBolt extends BaseRichBolt {
    private Producer producer;
    private OutputCollector collector;
    private TupleToKafkaMapper mapper;
    private KafkaTopicSelector topicSelector;
    private String statusFilter;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        if(this.mapper == null) {
            this.mapper = new FieldNameBasedTupleToKafkaMapper();
        }

        if(this.topicSelector == null) {
            this.topicSelector = new DefaultTopicSelector((String)stormConf.get("topic"));
        }

        Map configMap = (Map)stormConf.get("kafka.broker.properties");
        Properties properties = new Properties();
        properties.putAll(configMap);
        ProducerConfig config = new ProducerConfig(properties);
        this.producer = new Producer(config);
        this.collector = collector;
        this.statusFilter = (String) stormConf.get("statusFilter");
        this.statusFilter = this.statusFilter.trim();
    }

    @Override
    public void execute(Tuple input) {
        Object key = null;
        Object message = null;
        String topic = null;

        try {
            key = this.mapper.getKeyFromTuple(input);
            message =  input.getString(0);
            topic = this.topicSelector.getTopic(input);
            input.getString(0);
            if(topic != null) {
                if(message!=null){
                    String m = message+"";
                    JSONObject jsonObject = JSON.parseObject(m);
                    if(statusFilter.equals(jsonObject.get("STATUS"))){
                        this.producer.send(new KeyedMessage(topic, "n", m));

                    }
                }
            } else {
                System.out.println("skipping key = " + key + ", topic selector returned null.");
            }

            this.collector.ack(input);
        } catch (Exception var6) {
            this.collector.reportError(var6);
            this.collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
