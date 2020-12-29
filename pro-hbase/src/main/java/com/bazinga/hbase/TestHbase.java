package com.bazinga.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NamespaceExistException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class TestHbase {
    //1.构建Configuration, Connection, Admin
    //Configuration 持有了zk的信息，进而hbase集群的信息可以间接获得
    public static Configuration conf;
    //Connection  hbase连接  借助配置信息 获得连接
    public static Connection connection;
    public static Admin admin;
    static{  //为静态属性初始化，或者说辅助类初始化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "cdh19-49,cdh19-48,cdh19-47");
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //admin
        try {
            admin = connection.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException {
//        isExists("BAOFOO_SECURITY:Test_Web");

    }


    //1.创建库
    public static void createNS(String namespace) throws IOException {
        //①构建  ns的描述器  声明库名
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        //②创建库
        try{
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println("该库已经存在！");
        }
        //③关资源
        admin.close();
    }

    //2.判断表是否存在
    public static boolean isExists(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        System.out.println("exits:" + exists);
        admin.close();
        return exists;
    }

}