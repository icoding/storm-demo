package com.jndemo.storm.demo;


import com.jndemo.storm.demo.bolt.KafkaJnBolt;
import com.jndemo.storm.demo.common.ClassesConfigLoader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

public class KafkaMain {


    public static void main(String[] args){




        //producer 使用
        Map<String, String> map=new HashMap<String,String>();
        // 配置Kafka broker地址
        map.put("metadata.broker.list", "172.24.3.133:6667");
        // serializer.class为消息的序列化类
        map.put("serializer.class", "kafka.serializer.StringEncoder");

        Config conf = new Config();
        conf.setNumWorkers(2);
        conf.setDebug(false);
        conf.put("kafka.broker.properties", map);


        //kafka config
        //这个是kafka的地址，ip和port请执行填写
        String zks = "172.24.3.131:2181";
        //消息的topic
        //订阅的kafka的topic
        String topic = "apache-logs";
        //strom在zookeeper上的根
        //你再配置jstorm的时候指定的jstorm在zookeeper中的根目录
        String zkRoot = "/storm";
        //我们的user_id这个随便写
        String id = "jn_test";

        BrokerHosts brokerHosts = new ZkHosts(zks,"/kafka/brokers");


        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//////        spoutConf.forceFromStart = true;
////        //kafka地址，kafka-ip请自行填写
//        spoutConf.zkServers = Arrays.asList(new String[] {"172.24.3.131"});
////        //kafka端口
//        spoutConf.zkPort = 2181;
//        spoutConf.zkRoot = "/kafka";
        builder.setSpout("hello-spout",  new KafkaSpout(spoutConf));
        builder.setBolt("sout-bolt", new KafkaJnBolt()).shuffleGrouping("hello-spout");

        if ("local".equals(ClassesConfigLoader.getProperty("storm.execution.mode"))) {
            // 配置KafkaBolt生成的topic
            conf.put("topic", "jntest");
            conf.put("statusFilter","200");
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 20);
            conf.setNumWorkers(3);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("jn", conf, builder.createTopology());

            Utils.sleep(100000);
            cluster.killTopology("jn");

            cluster.shutdown();
        }




        if ("cluster".equals(ClassesConfigLoader.getProperty("storm.execution.mode"))) {
            conf.put("topic", args[1]);
            conf.put("statusFilter",args[2]);
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 20);
            conf.setNumWorkers(3);
            try {
                StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }

        }

    }
}
