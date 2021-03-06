package com.examples.hdfs.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
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

    //上传文件
    @Test
    public void TestUpload() throws IOException {
        fs.copyFromLocalFile(false, true,
                new Path("C:\\Users\\james\\Desktop\\深度学习和数据分析.docx"), new Path("/"));
    }

    //下载文件
    @Test
    public void TestDownload() throws IOException {
        fs.copyToLocalFile(false, new Path("/深度学习和数据分析.docx"),
                new Path("d:/111"), true);
    }

    //删除文件
    @Test
    public void TestDelete() throws IOException {
        fs.delete(new Path("/深度学习和数据分析.docx"), true);
    }
    //重命名
    @Test
    public void TestRename() throws IOException {
        fs.rename(new Path("/IEDA"), new Path("/IDEA"));
    }
    //判断当前文件是否存在
    @Test
    public void TestIfPathExists() throws IOException {
        System.out.println(fs.exists(new Path("/IDES")));
    }

    //判断是文件还是路径
    @Test
    public void TestFileIsDir() throws IOException {
        Path path = new Path("/user");
//        FileStatus fileStatus = fs.getFileStatus(path);

        FileStatus[] fileStatuses = fs.listStatus(path);

        for (FileStatus f: fileStatuses) {
            //path1包含协议名
            Path path1 = f.getPath();
            System.out.println(path1.getName()+"是否是目录"+f.isDirectory());
            System.out.println(path1.getName()+"是否是文件"+f.isFile());
        }
    }

    //获取文件的块信息
    @Test
    public void TestGetBlockInfomation() throws IOException {
        Path path = new Path("/深度学习和数据分析.docx");
        RemoteIterator<LocatedFileStatus> status = fs.listLocatedStatus(path);

        while (status.hasNext()){
            LocatedFileStatus locatedFileStatus = status.next();
            System.out.println(locatedFileStatus.getOwner());
            System.out.println(locatedFileStatus.getGroup());

            BlockLocation[] blockLocations = locatedFileStatus.getBlockLocations();
            for (BlockLocation blockLocatin: blockLocations) {
                System.out.println(blockLocatin);
                System.out.println("========================");
            }
        }
    }
}
