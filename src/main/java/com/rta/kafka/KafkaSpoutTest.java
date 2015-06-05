package com.rta.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
 
public class KafkaSpoutTest implements IRichSpout {
    private SpoutOutputCollector collector;
    private ConsumerConnector consumer;
    private String topic;
 
    public KafkaSpoutTest() {}
     
    public KafkaSpoutTest(String topic) {
        this.topic = topic;
    }
 
    public void nextTuple() {    }
 
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }
 
    public void ack(Object msgId) {    }
 
    public void activate() {         
        consumer =kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig()); 
        Map<String,Integer> topickMap = new HashMap<String, Integer>();  
        topickMap.put(topic, 1);  
 
        System.out.println("*********Results********topic:"+topic);  
 
        Map<String, List<KafkaStream<byte[],byte[]>>>  streamMap=consumer.createMessageStreams(topickMap);  
        KafkaStream<byte[],byte[]>stream = streamMap.get(topic).get(0);  
        ConsumerIterator<byte[],byte[]> it =stream.iterator();   
        while(it.hasNext()){  
             String value =new String(it.next().message());
             SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd日 HH:mm:ss SSS");
             Date curDate = new Date(System.currentTimeMillis());       
             String str = formatter.format(curDate);   
             System.out.println("storm接收到来自kafka的消息--->" + value);
             collector.emit(new Values(value,1,str), value);
        }  
    }
     
    private static ConsumerConfig createConsumerConfig() {  
        Properties props = new Properties(); 
        props.put("zookeeper.connect","localhost:2181");
        props.put("group.id", "1");  
        // 间隔更新zookeeper上关于kafka的group消费记录，非实时更新
        props.put("auto.commit.interval.ms", "1000");
        props.put("zookeeper.session.timeout.ms","10000");  
        return new ConsumerConfig(props);  
    }  
 
    public void close() {    }
 
    public void deactivate() {    }
 
    public void fail(Object msgId) {    }
 
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","id","time"));
    }
 
    public Map<String, Object> getComponentConfiguration() {
        System.out.println("getComponentConfiguration被调用");
        // bin/kafka-topics.sh --list --zookeeper localhost:2181
        topic="test";
        return null;
    }
}
