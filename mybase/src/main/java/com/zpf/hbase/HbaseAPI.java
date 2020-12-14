package com.zpf.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseAPI {
    public static void main(String[] args) throws IOException {
        //获取配置
        final Configuration cfg = HBaseConfiguration.create();
        //创建连接
        final Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();


    }
}
