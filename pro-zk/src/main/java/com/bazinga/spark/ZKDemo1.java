package com.bazinga.spark;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKDemo1 {
    private static final String CONNECT_STRING = "CDH19-47:2181,CDH19-48:2181,CDH19-49:2181";
    //如果zookeeper使用的是默认端口的话，此处可以省略端口号
    //private static final String CONNECT_STRING = "hadoop1,hadoop2,hadoop3";
    //设置超时时间
    private static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(CONNECT_STRING, SESSION_TIMEOUT, null);
//        String create = zk.create("/aa", "hello world!".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
//        System.out.println(create);
        byte[] data = zk.getData("/aa0001407832", true, null);
        String s = new String(data);
        System.out.println(s);

//        Stat exists = zk.exists("/aa0001407815", null);
//        if(exists == null){
//            System.out.println("节点不存在！");
//        }else{
//            System.out.println("节点存在！");
//        }
//        Stat stat = zk.setData("/aa0001407815", "xyz".getBytes(), -1);
//        if (stat == null) {
//            System.out.println("节点不存在 --- 修改不成功");
//        }else {
//            System.out.println("节点存在 --- 修改成功");
//        }
//        zk.delete("/aa0001407815",-1);

    }
}
