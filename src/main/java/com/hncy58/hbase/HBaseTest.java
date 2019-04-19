package com.hncy58.hbase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import java.util.ArrayList;
import java.util.List;
 
public class HBaseTest {
    private static final String nodes = "192.168.3.104";
 
    public static void main(String[] args) {
        try{
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum",nodes);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            HBaseService hBaseService = new HBaseService(conf);
            List<String> cf = new ArrayList<>();
            cf.add("cf1");
            cf.add("cf2");
            cf.add("cf3");
            hBaseService.creatTable("test",cf);
        }catch(Exception e){
            e.printStackTrace();
        }
 
    }
}
