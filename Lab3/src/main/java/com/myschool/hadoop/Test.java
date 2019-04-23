package com.myschool.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
 
import java.io.InputStream;
import java.net.URI;
 
public class Test {
    public static void main(String[] args) throws Exception {
        String uri = "hdfs://localhost:9000/";
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), config);
 
        // 列出hdfs上/user/hadoop/目录下的所有文件和目录
        FileStatus[] statuses = fs.listStatus(new Path("/user/hadoop"));
        for (FileStatus status : statuses) {
            System.out.println(status);
        }
 
        // 在hdfs的/user/hadoop目录下创建一个文件，并写入一行文本
        FSDataOutputStream os = fs.create(new Path("/user/hadoop/test.log"));
        os.write("Hello World!".getBytes());
        os.flush();
        os.close();
 
        // 显示在hdfs的/user/hadoop下指定文件的内容
        InputStream is = fs.open(new Path("/user/hadoop/test.log"));
        IOUtils.copyBytes(is, System.out, 1024, true);
    }
}
