package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase05 {
    public static void main(String[] args) throws IOException {
        // 使用Hbase的API访问Hbase数据库
        // 建立ZK的连接配置,用于连接Hbase
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        // 获取连接对象
        final Connection connection = ConnectionFactory.createConnection(cfg);
        // 获取到表
        final TableName student = TableName.valueOf("atguigu:student");
        final Table table = connection.getTable(student);
        // 给表增加数据(需要创建put对象,并制定rowkey)
        String rowkey="1001";
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes("wangwu")
        );
        put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes("20")
        );
        put.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("gender"),
                Bytes.toBytes("man")
        );
        table.put(put);
        System.out.println("新增数据成功");
        table.close();
        connection.close();
    }
}
