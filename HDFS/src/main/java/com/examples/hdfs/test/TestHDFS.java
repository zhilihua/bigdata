package com.examples.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
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
    private Configuration conf;
    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统
        conf = new Configuration();
        //配置集群参数
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
    }

    @After
    public void close() throws IOException {
        if (fs != null)
            fs.close();
    }

    @Test
    public void TestMkdir() throws IOException {
        // 创建目录
        fs.mkdirs(new Path("/IEDA"));
    }

    @Test
    public void TestUpload() throws IOException {
        fs.copyFromLocalFile(false, true,
                new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析.docx"), new Path("/"));
    }

    @Test
    public void TestDownload() throws IOException {
        fs.copyToLocalFile(false, new Path("/深度学习和数据分析.docx"),
                new Path("d:/111"), true);
    }
}
