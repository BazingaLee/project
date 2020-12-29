package com.bazinga.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHbase {
    //1.构建Configuration, Connection, Admin
    //Configuration 持有了zk的信息，进而hbase集群的信息可以间接获得
    public static Configuration conf;
    //Connection  hbase连接  借助配置信息 获得连接
    public static Connection connection;
    public static Admin admin;

    static {  //为静态属性初始化，或者说辅助类初始化
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
//         createNS("habasedemo");
//        createTable("habasedemo:student","name");
//        queryAll("student");

        //put 'habasedemo:student','1001','name','bazinga'
        insertData("student", "1004", "cf1:name", "111111");
        getRow("student", "1004");
    }


    //1.创建库
    public static void createNS(String namespace) throws IOException {
        //①构建  ns的描述器  声明库名
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        //②创建库
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
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

    //3.创建表
    public static void createTable(String tableName, String... info) throws IOException {
        //①HTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //②添加columnFamily 列族
        for (String cf : info) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //③建表
        admin.createTable(hTableDescriptor);
        //④释放资源
        admin.close();
    }

    //4.deleteTable
    public static void deleteTable(String tableName) throws IOException {
        //禁用并删除表
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
    }

    //5.插入数据  put 'student','1001','cf1:name','kris'
    public static void insertData(String tableName, String rowkey, String column, String value) throws IOException {
        //①获取table
        Table table = connection.getTable(TableName.valueOf(tableName));
        //②获得put
        Put put = new Put(Bytes.toBytes(rowkey));//把String类型转成bytes类型
        put.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]),
                Bytes.toBytes(value));
        table.put(put); //③添加数据if (table.isAutoFlush())
        table.close();//④释放资源
    }

    //6.删除数据
    public static void deleteData(String tableName, String... rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rk : rowkey) {
            Delete del = new Delete(Bytes.toBytes(rk));//获得delete对象，其中持有要删除行的rowkey
            table.delete(del);
        }
        table.close();
    }

    //7.查询
    //　　等到后续，在仔细看错误，发现，Caused by: java.net.UnknownHostException: host-10-191-36-24，找不到主机的异常，才突然明白，应用程序首先连接到zk，然后zk告知region在哪个regionserver上，然后，应用程序再连接到hbase的regionserver上读写数据。
    public static void queryAll(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) { //result对应一行数据
            Cell[] cells = result.rawCells(); //获取一行的所有cells
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));//
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey:" + rowkey + "\t" + family + ":" + column
                        + "\t" + value);
            }
        }
    }

    //8.查询单行
    public static void getRow(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
        //get.addFamily(Bytes.toBytes("cf1")); //如果不追加列族，则查询所有列族
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("查询单行");
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("row:" + row + "\t" + family + ":" + column + "\t" + value);
        }
    }

}