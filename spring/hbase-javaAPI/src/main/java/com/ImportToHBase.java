package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class ImportToHBase {
    public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException  {
        String TABLE="identifyrmbrecords";
        Path inputDir = new Path("/usr/txt/stumer_in_out_details.txt");
        Configuration conf = new Configuration();
        String jobName = "Import to "+ TABLE;
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ImportToHBase.class);  // ����Driver��
        FileInputFormat.setInputPaths(job, inputDir);  // �������·��
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ImportMapper.class);  // ����Mapper��
        TableMapReduceUtil.initTableReducerJob(
                TABLE,  // ���ӵı���
                null,  // ����Reducer�����ʽ����Ϊû��Reduce������û�����ֵ
                job);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
