package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase02 {
    public static void main(String[] args) throws IOException {
        //TODO 使用Hbase的API访问Hbase数据库
        // 建立zk的连接配置,用于连接Hbase
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //TODO 获取连接对象
        Connection connection= ConnectionFactory.createConnection(cfg);
        final Admin admin=connection.getAdmin();
        //TODO :判断表是否存在
        // 获取表名时,如果表在命名空间中,必须是命名空间同时指定,使用冒号分隔
        // 如果命名空间不存在,不会发生错误,而是返回false
        final TableName student=TableName.valueOf("atguigu:student");
        final boolean b=admin.tableExists(student);
        if(b){
            System.out.println("表已经存在");
        }else{
            System.out.println("表不存在");
        }
        admin.close();
        connection.close();
    }
}
