package com.rta.nginxlog;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

// 访问超时数量Bolt
public class TimeoutCountBolt extends BaseBasicBolt {

    public static Logger LOG = LoggerFactory.getLogger(TimeoutCountBolt.class);

    private Map<String, Integer> method2Count;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        method2Count = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String method = input.getString(0);
        int count = input.getInteger(1);

        if (!method2Count.containsKey(method)) {
            method2Count.put(method, 0);
        }

        int value = method2Count.get(method);
        int newValue = count + value;
        method2Count.put(method, newValue);

        String emitinfo = String.format("%s=%d", method, newValue);
        LOG.debug("method2Count {}", emitinfo);
        System.out.println("TimeoutBolt获取的信息------->" + emitinfo);

        //发送request=count数据到Kafka/mysql
        collector.emit(new Values(String.format("%s=%d", method, newValue)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("kafka-method2Count"));
    }
}