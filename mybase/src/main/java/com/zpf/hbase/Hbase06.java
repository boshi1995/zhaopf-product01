package com.zpf.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.shaded.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class Hbase06 {
    public static void main(String[] args) throws IOException {
        //使用Hbase的API访问Hbase数据库
        final Configuration cfg = HBaseConfiguration.create();
        //建立ZK的连接配置
        cfg.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //获取连接对象
        final Connection connection = ConnectionFactory.createConnection(cfg);
        //根据namespace及表名来获取表
        final TableName student = TableName.valueOf("atguigu:student");
        final Table table = connection.getTable(student);
        //查询单条数据
        //获得rowkey
        Get rowkey=new Get(Bytes.toBytes("1001"));
        final Result result = table.get(rowkey);
        //获得存储的一列小格子
        final Cell[] cells = result.rawCells();
        //遍历这一列小格子
        for(Cell cell:cells){
            System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        System.out.println("*****************************************");
        //查询全表数据
        final Scan scan = new Scan();
        final ResultScanner scanner = table.getScanner(scan);
        final Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            final Result next = iterator.next();
            final Cell[] cells1=result.rawCells();
            for(Cell cell:cells1){
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
        connection.close();
    }
}
