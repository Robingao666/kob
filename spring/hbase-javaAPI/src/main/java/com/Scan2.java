package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;

public class Scan2 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "hadoop1:16000"); //配置连接的HMaster
        conf.set("hbase.rootdir", "hdfs://hadoop1:9000/hbase"); //配置HBase数据在HDFS上的存储路径
        conf.set("hbase.zookeeper.quorum", "hadoop1"); //配置HBase使用的zookeeper集群服务器
        conf.set("hbase.zookeeper.property.clientPort", "2181"); //配置zookeeper端口
        Connection conn = ConnectionFactory.createConnection(conf);	//��ȡ����
        Table table = conn.getTable(TableName.valueOf("identify_rmb_records1"));
        Scan scan = new Scan();
        scan.addFamily("op_www".getBytes());
        scan.setMaxVersions();  // ��ȡ����汾
        scan.setStartRow("AABX0673".getBytes());
        scan.setMaxVersions(2);

        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> reslut = scanner.iterator();
        while (reslut.hasNext()) {
            Result re = reslut.next();
            for (Cell cell:re.rawCells()) {  // ����Result�е�����
                System.out.println();
                System.out.print(new String(CellUtil.cloneRow(cell))+"|");  // ��ӡrowkey
                System.out.print(new String(CellUtil.cloneFamily(cell))+"|");  // ��ӡ�д�
                System.out.print(new String(CellUtil.cloneQualifier(cell))+"|");	 // ��ӡ����
                System.out.print(new String(CellUtil.cloneValue(cell)));  // ��ӡ����ֵ
            }
        }
    }

}
