package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class createHBaseTable {
	// ����HBase����������Ʊ��
	public static void main(String[] args) throws IOException {
		String tableName = "identify_rmb_records1";
		//建立与HBase的连接
		Configuration conf = HBaseConfiguration.create(); //实例化Configuration
		//配置与HBase连接的参数
		conf.set("hbase.master", "hadoop1:16000"); //配置连接的HMaster
		conf.set("hbase.rootdir", "hdfs://hadoop1:9000/hbase"); //配置HBase数据在HDFS上的存储路径
		conf.set("hbase.zookeeper.quorum", "hadoop1"); //配置HBase使用的zookeeper集群服务器
		conf.set("hbase.zookeeper.property.clientPort", "2181"); //配置zookeeper端口
		Connection conn = ConnectionFactory.createConnection(conf);  // ��ȡ����
		Admin admin = conn.getAdmin();  // ����Adminʵ��
		TableName tablename = TableName.valueOf(tableName);  // ��������
		HTableDescriptor ht = new HTableDescriptor(tablename);  // �ñ�������HTableDescriptor����
		byte[][] Regions = new byte[][] {
				Bytes.toBytes("AAAR3333"),
				Bytes.toBytes("AABI6666")
		};
		ht.addFamily(new HColumnDescriptor("op_www").setMaxVersions(1000));  // �����д��������汾��
		admin.createTable(ht,Regions);  // �������ݱ�
	}

}
