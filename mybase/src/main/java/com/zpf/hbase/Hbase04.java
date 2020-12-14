package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase04 {
    public static void main(String[] args) throws IOException {
        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        Connection connection= ConnectionFactory.createConnection(cfg);
        final Admin admin=connection.getAdmin();

        //TODO 删除表
        // 删除表之前要将表禁用掉,防止有其他的操作
        // 如果表不存在,会发生错误:TableNotFoundException
        final TableName student=TableName.valueOf("atguigu:student");
        final boolean b=admin.tableExists(student);
        if(b){
            System.out.println("表已经存在");
            admin.disableTable(student);
            admin.deleteTable(student);
            System.out.println("表删除成功");
        }
        admin.close();
        connection.close();
    }
}
