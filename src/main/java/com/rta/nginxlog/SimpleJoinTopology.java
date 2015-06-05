package com.rta.nginxlog;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.testing.FeederSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.rta.kafka.KafkaSpoutTest;

public class SimpleJoinTopology {
    public static void main(String[] args) {
        NginxSplitBolt nginxBolt = new NginxSplitBolt();
        ServiceLogBolt serviceBolt = new ServiceLogBolt();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("nginx", new KafkaSpoutTest("log.accesslog"), 1);
        builder.setSpout("service", new KafkaSpoutTest("log.servicelog"), 1);

        builder.setBolt("nginxlog", nginxBolt).shuffleGrouping("nginx");
        builder.setBolt("servicelog", serviceBolt).shuffleGrouping("service");

        builder.setBolt("join", new SingleJoinBolt(new Fields("method", "time", "usetime", "params")))
                .fieldsGrouping("nginxlog", new Fields("ip", "utime"))
                .fieldsGrouping("servicelog", new Fields("ip", "utime"));

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log - join", conf, builder.createTopology());

        Utils.sleep(2000);
        cluster.shutdown();
    }
}
