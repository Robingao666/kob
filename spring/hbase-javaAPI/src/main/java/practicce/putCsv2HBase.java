
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class putCsv2HBase {
    public static void main(String[] args) throws IOException {
        // 设置HBase配置信息
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.master", "master:16000");  // 指定HMaster
        conf.set("hbase.rootdir", "hdfs://hadoop1:8020/hbase");  // 指定HBase在hdfs上的储存路径
        conf.set("hbase.zookeeper.quorum", "hadoop1");  // 指定使用的zookeeper集群
        conf.set("hbase.zookeeper.property.clientPort", "2181");  // 指定zookeeper端口
        Connection conn = ConnectionFactory.createConnection(conf);      // 获取连接
        // 获取表对象
        Table table = conn.getTable(TableName.valueOf("newData"));
        // 读取CSV文件
        CSVReader reader = new CSVReader(new FileReader("C:\\Users\\zhuaba\\Desktop\\newdata.csv"));
        String[] nextLine;
        // 创建Put对象列表
        List<Put> puts = new ArrayList<>();
        // 逐行处理CSV数据
        while ((nextLine = reader.readNext()) != null) {
            // 获取行键、列族和列限定符
            String rowKey = nextLine[0];
            String columnFamilyValue = nextLine[1];
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes("data"),
                    Bytes.toBytes("data"),
                    Bytes.toBytes(nextLine[1]));
            puts.add(put);
        }
        // 将数据批量写HBase
        table.put(puts);
        // 关闭资源
        reader.close();
        table.close();
        conn.close();
    }
}
