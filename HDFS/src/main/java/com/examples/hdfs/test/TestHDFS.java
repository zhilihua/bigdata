package com.examples.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description: 测试创建文件，在HDFS上
 * @Auther: HuaZhiLi
 * @Date: 2020/11/24 23:07
 */
public class TestHDFS {
    @Test
    public void TestMkdir() throws IOException, InterruptedException, URISyntaxException {
        //1 获取文件系统
        Configuration conf = new Configuration();
        //配置集群参数
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2 创建目录
        fs.mkdirs(new Path("/IEDA"));

        fs.close();
    }
}
