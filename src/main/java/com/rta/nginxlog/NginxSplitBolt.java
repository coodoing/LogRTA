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

public class NginxSplitBolt extends BaseBasicBolt { //FeederSpout

    private Fields outFields;

    public static Logger LOG = LoggerFactory.getLogger(NginxSplitBolt.class);
    public static final String RTA_DOMAIN = "www.rta.com";

    public NginxSplitBolt(){
        Fields filed = new Fields("method", "count", "ip", "time", "userid", "request", "utime");
        outFields = filed;
    }

    public NginxSplitBolt(Fields outFields){
        outFields = outFields;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        //获取spout发出的第一个字段。是文本日志，所以此处选择getString(int index).
        // Value 是 list 类型
        String json = input.getString(0);
        System.out.println("log日志json：------->" + json);
        JSONObject obj = (JSONObject) JSON.parse(json);
        if(obj.size() != 0){
            String domain = (String)obj.get("domain");
            String request = (String)obj.get("mthd_url");
            boolean openin = domain.equals(NginxSplitBolt.RTA_DOMAIN);
            boolean validRequest = "" != request && null != request;

            if(openin && validRequest){

                int status = obj.get("status") == null ? 499 : Integer.parseInt((String)obj.get("status"));
                String ip = (String)obj.get("ip");
                String time = (String)obj.get("srvtime");
                String utime = (String)obj.get("srvtime"); //统一时间，用于join过滤优化，根据time获取
                //int userid = obj.get("userid")==null ? -1 : Integer.parseInt((String)obj.get("userid"));
                // userid 很大，会超出int表示范围
                String userid = obj.get("userid")==null ? "" : (String)obj.get("userid");

                // action正则
                String pattern = "/action/([^\\\\/\\\\?]+)";
                // 创建 Pattern 对象
                Pattern r = Pattern.compile(pattern);
                // 现在创建 matcher 对象
                Matcher m = r.matcher(request);

                if (m.find()) {
                    // 访问接口
                    String method = m.group(1);
                    if(status == 499 || status != 200){
                        String emitinfo = String.format("method=%s,count=%d,ip=%s,time=%s,userid=%s", method, 1, ip, time, userid);
                        LOG.debug("nginxlog {}",  emitinfo);
                        System.out.println("nginxlog------->" + emitinfo);
                        collector.emit(new Values(method, 1, ip, time, userid, request, utime));
                    }
                }
            }
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

    // 解析actionRequest字段
    private void parseActionRequest(String request){

    }

    private boolean isActionRequest(String request){
        String pattern = "/action/([^\\\\/\\\\?]+)";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(request);
        if (m.find()) {
            return true;
        }
        return false;
    }

    /**
     * fastjson 解析函数
     *
     * @param String input
     * @return List<Map<String, String>>
     * */
    private List<Map<String, String>> jsonParse(String input){
        List<String> split = JSON.parseArray(input, String.class);//JSON.parseObject(input, NginxAccessLog.class);
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        for(int i = 0; i < split.size(); i++) {
            String tmp = split.get(i);
            if(tmp != null && !tmp.isEmpty()){
                String[] pairs = tmp.split(":");
                Map<String, String> map = new HashMap<String, String>();
                map.put(pairs[0], pairs[1]);

                list.add(map);
            }
        }
        return list;
    }
}