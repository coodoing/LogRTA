package com.rta.nginxlog;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ServiceLogBolt extends BaseBasicBolt {

    private Fields outFields;
    public static Logger LOG = LoggerFactory.getLogger(NginxSplitBolt.class);

    public ServiceLogBolt(){
        Fields filed = new Fields("method", "ip", "time", "userid", "usetime", "params", "utime");
        outFields = filed;
    }

    public ServiceLogBolt(Fields outFields){
        outFields = outFields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String json = input.getString(0);
        JSONObject obj = (JSONObject) JSON.parse(json);

        if(obj.size() != 0){

            String method = "";
            String ip = "";
            String time = "";
            String utime = "";
            String userid = "";
            String usetime = "";
            String params = "";

            //TODO
            System.out.println("ServiceLogBolt解析出来的信息------->" + "infoinfo");
            collector.emit(new Values(method, ip, time, userid, usetime, params, utime));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(outFields);
    }

    @Override
    public String toString(){
        return "";
    }
}
