package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase03 {

    public static void main(String[] args) throws IOException {

        final Configuration cfg = HBaseConfiguration.create();
        cfg.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
        Connection connection = ConnectionFactory.createConnection(cfg);
        final Admin admin = connection.getAdmin();
        final TableName student = TableName.valueOf("atguigu:student");
        final boolean b = admin.tableExists(student);
        if (b) {
            System.out.println("表已经存在");
        } else {
            System.out.println("表不存在");
            final TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(student);
            String cfName = "info";
            byte[] bs = Bytes.toBytes(cfName);
            final ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder =
                    ColumnFamilyDescriptorBuilder.newBuilder(bs);
            builder.setColumnFamily(columnFamilyDescriptorBuilder.build());
            admin.createTable(builder.build());
            System.out.println("表创建成功");
        }
        admin.close();
        connection.close();
    }
}
