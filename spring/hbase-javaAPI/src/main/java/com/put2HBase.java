package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class put2HBase {
    // ����HBase���ݿ⣬�������ݵ���Ʊ����ѯ�����������ݺͲ�ѯ����
    public static void main(String[] args) throws IOException, ParseException {
        String tableName = "identify_rmb_records1";
        String line;
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "hadoop1:16000"); //配置连接的HMaster
        conf.set("hbase.rootdir", "hdfs://hadoop1:9000/hbase"); //配置HBase数据在HDFS上的存储路径
        conf.set("hbase.zookeeper.quorum", "hadoop1"); //配置HBase使用的zookeeper集群服务器
        conf.set("hbase.zookeeper.property.clientPort", "2181"); //配置zookeeper端口
        Connection conn = ConnectionFactory.createConnection(conf);	  // ��ȡ����
        Table table = conn.getTable(TableName.valueOf(tableName));

        SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm");
        // ���ж�ȡ����
        BufferedReader br = new BufferedReader(new FileReader("C:\\Users\\zhuaba\\Desktop\\3.3data\\data\\stumerinoutdetails.txt"));
        while ((line = br.readLine())!=null) {
            String[] lines = line.split(",");
            Put put = new Put(lines[0].getBytes());
            long timeStamp = format.parse(lines[2]).getTime();
            // �������cell��Ӧ�ľ���ֵ
            put.addColumn("op_www".getBytes(),"exist".getBytes(),timeStamp, lines[1].getBytes());
            put.addColumn("op_www".getBytes(),"Bank".getBytes(),timeStamp, lines[3].getBytes());
            if (lines.length == 4) {
                put.addColumn("op_www".getBytes(),"uId".getBytes(),timeStamp, "".getBytes());
            }else {
                put.addColumn("op_www".getBytes(),"uId".getBytes(),timeStamp, lines[4].getBytes());
            }
            table.put(put);
        }
        br.close();
    }

}
