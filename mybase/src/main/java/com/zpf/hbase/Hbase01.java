package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase01 {
    public static void main(String[] args) throws IOException {
        //TODO 使用Hbase的API访问Hbase数据库
        // 建立zk的连接配置,用于连接Hbase
        final Configuration cfg = HBaseConfiguration.create();
        //cfg.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //TODO 获取连接对象
        Connection connection= ConnectionFactory.createConnection(cfg);
        final Admin admin=connection.getAdmin();
        //TODO 命名空间的操作
        // 创建命名空间
        final NamespaceDescriptor ns=NamespaceDescriptor.create("newspace").build();
        admin.createNamespace(ns);
        admin.close();
        connection.close();
    }
}
