package com.examples.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Description:
 * @Auther: HuaZhiLi
 * @Date: 2020/11/27 21:49
 */
public class TestCustomUploadAndDownload {
    private Configuration conf;
    private FileSystem fs;
    private FileSystem localFs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        //1 获取文件系统
        conf = new Configuration();
        //配置集群参数
        fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");
        //本地文件系统
        localFs = FileSystem.get(conf);
    }

    @Test
    public void TestCustomUpload() throws IOException {
        Path src = new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析.docx");
        Path dst = new Path("/深度学习和数据分析1.docx");

        //本地文件创建一个输入流
        FSDataInputStream is = localFs.open(src);
        //hdfs文件系统创建一个输出流
        FSDataOutputStream os = fs.create(dst, true);
        //流的拷贝
        byte[] buffer = new byte[1024];   //1K大小数据
        for (int i=0; i < 100; i++){
            is.read(buffer);
            os.write(buffer);
        }
        //关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);

        fs.close();
    }

    @Test
    public void TestFirstBlock() throws IOException {   //下载第一块
        Path src = new Path("/深度学习和数据分析1.docx");
        Path dst = new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析first");
        //本地文件创建一个输入流
        FSDataInputStream is = fs.open(src);
        //hdfs文件系统创建一个输出流
        FSDataOutputStream os = localFs.create(dst, true);

        //流的拷贝
        byte[] buffer = new byte[1024];   //1K大小数据
        for (int i=0; i < 100; i++){
            is.read(buffer);
            os.write(buffer);
        }

        //关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);

        fs.close();
    }

    @Test
    public void TestSecondBlock() throws IOException {  //下载第二块
        Path src = new Path("/深度学习和数据分析1.docx");
        Path dst = new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析second");
        //本地文件创建一个输入流
        FSDataInputStream is = fs.open(src);
        //hdfs文件系统创建一个输出流
        FSDataOutputStream os = localFs.create(dst, true);

        //定位位置
        is.seek(1024 * 100);
        //流的拷贝
        byte[] buffer = new byte[1024];   //1K大小数据
        for (int i=0; i < 100; i++){
            is.read(buffer);
            os.write(buffer);
        }

        //关流
        IOUtils.closeStream(is);
        IOUtils.closeStream(os);

        fs.close();
    }

    @Test
    public void TestFinallyBlock() throws IOException {  //下载最后一块
        Path src = new Path("/深度学习和数据分析1.docx");
        Path dst = new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析second");
        //本地文件创建一个输入流
        FSDataInputStream is = fs.open(src);
        //hdfs文件系统创建一个输出流
        FSDataOutputStream os = localFs.create(dst, true);

        //定位位置
        is.seek(1024 * 100 * 2);
        //流的拷贝,buffSize不能超过4096
        IOUtils.copyBytes(is, os, 4096, true);

        fs.close();
    }

}
