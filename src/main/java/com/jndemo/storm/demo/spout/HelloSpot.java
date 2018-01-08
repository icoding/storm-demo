package com.jndemo.storm.demo.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.util.Map;
import java.util.Random;


public class HelloSpot extends BaseRichSpout {

    protected Logger log = LoggerFactory.getLogger(this.getClass());

    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;


    /**
     * open()在spout实例化后被调用。
     */
    @Override
    public void open(Map stormConf, TopologyContext context,
                     SpoutOutputCollector collector) {
        log.info("  >> prepare(), task.id = {}  component.id = {}  ", context.getThisTaskId(), context.getThisComponentId());

        this.context = context;
        this.collector = collector;
    }
    private static String[] words = {"happy","excited","angry"};


    /**
     * Declare the output field "word"
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }


    /**
     * nextTuple()是一个持续不断被调用的方法；它用来发送tuple(消息)。
     * <p/>
     * 对于spout产生的每一个tuple，后面还会有多个bolt对其进行处理，storm会进跟踪这些bolt的处理结果。
     * 如果有一个bolt对这个tuple的处理fail了，会立即触发spout的fail()；
     * 只有当所有的bolt都处理成功了，才会触发spout的ack()；
     */
    @Override
    public void nextTuple() {
        String word = words[new Random().nextInt(words.length)];
        collector.emit(new Values(word));
    }
    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        log.info("  >> {}({}) ack: " + msgId, context.getThisTaskId(), context.getThisComponentId());
    }
    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        log.info("  >> {}({}) fail: " + msgId, context.getThisTaskId(), context.getThisComponentId());

        // 开启这行试试失败重发
        //this.collector.emit(new Values(msgId), msgId);
    }
    @Override
    public void close() {
        super.close();
        log.info("  >> {}({}) close: ", context.getThisTaskId(), context.getThisComponentId());
    }

}
