package com.examples.zookeeper.test;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class TestZK {
    private String connectString="hadoop102:2181,hadoop103:2181,hadoop104:2181";
    private int sessionTimeout=6000;
    private ZooKeeper zooKeeper;

    @Before
    public void init() throws Exception {
        //创建一个zk客户端对象
        zooKeeper = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });

        System.out.println(zooKeeper);
    }

    @After
    public void close() throws InterruptedException {
        if (zooKeeper != null)
            zooKeeper.close();
    }

    @Test
    public void ls() throws KeeperException, InterruptedException {
        Stat stat = new Stat();

        List<String> children = zooKeeper.getChildren("/", null, stat);

        System.out.println(children);
        System.out.println(stat);
    }

    @Test
    public void create() throws KeeperException, InterruptedException {
        zooKeeper.create("/IDEA", "idea".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        byte[] data = zooKeeper.getData("/IDEA", null, null);
        System.out.println(new String(data));
    }

    @Test
    public void set() throws KeeperException, InterruptedException {
        zooKeeper.setData("/IDEA", "hi".getBytes(), -1);
    }

    @Test
    public void delete() throws Exception {

        zooKeeper.delete("/IDEA", -1);

    }

    // rmr path
    @Test
    public void rmr() throws Exception {

        String path="/data";

        //先获取当前路径中所有的子node
        List<String> children = zooKeeper.getChildren(path, false);

        //删除所有的子节点
        for (String child : children) {

            zooKeeper.delete(path+"/"+child, -1);

        }

        zooKeeper.delete(path, -1);

    }

    // 判断当前节点是否存在
    @Test
    public void ifNodeExists() throws Exception {

        Stat stat = zooKeeper.exists("/data2", false);

        System.out.println(stat==null ? "不存在" : "存在");

    }
}
