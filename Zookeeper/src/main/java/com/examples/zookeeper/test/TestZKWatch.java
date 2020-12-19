package com.examples.zookeeper.test;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestZKWatch {
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
    public void lsAndWatch() throws KeeperException, InterruptedException {
        //传入true，默认使用客户端自带的观察者
        zooKeeper.getChildren("/data2", new Watcher() {
            //当前线程自己设置观察者
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event.getPath()+"发生如下事件："+event.getType());

                List<String> children;
                try {
                    children = zooKeeper.getChildren("/data2", null);
                    System.out.println(event.getPath()+"的新节点:"+children);
                }catch (KeeperException e){
                    e.printStackTrace();
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
            }
        });

        //客户端所在的进程不能死亡
        while (true) {
            Thread.sleep(5000);
            System.out.println("我还活着......");
        }
    }

    private CountDownLatch cdl=new CountDownLatch(1);

    // 监听器的特点： 只有当次有效
    // get path watch
    @Test
    public void getAndWatch() throws Exception {
        //zooKeeper = new ZooKeeper(connectString, sessionTimeout, true)  会自动调用Before里面的process
        //是Connect线程调用
        byte[] data = zooKeeper.getData("/data2", new Watcher() {

            // 是Listener线程调用
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                //减一
                cdl.countDown();
            }
        }, null);

        System.out.println("查询到的数据是:"+new String(data));

        //阻塞当前线程，当初始化的值变为0时，当前线程会唤醒
        cdl.await();

    }

    // 持续watch：  不合适
    @Test
    public void lsAndAlwaysWatch() throws Exception {

        //传入true,默认使用客户端自带的观察者
        zooKeeper.getChildren("/data2",new Watcher() {

            // process由listener线程调用，listener线程不能阻塞,阻塞后无法再调用process
            //当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                System.out.println(Thread.currentThread().getName()+"---->我还活着......");

                try {
                    lsAndAlwaysWatch();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });

        //客户端所在的进程不能死亡
        while(true) {

            Thread.sleep(5000);

            System.out.println(Thread.currentThread().getName()+"---->我还活着......");

        }

    }

    // 持续watch：  不合适
    @Test
    public void testLsAndAlwaysWatchCurrent() throws Exception {

        lsAndAlwaysWatchCurrent();

        //客户端所在的进程不能死亡
        while(true) {

            Thread.sleep(5000);

            System.out.println(Thread.currentThread().getName()+"---->我还活着......");

        }

    }

    @Test
    public void lsAndAlwaysWatchCurrent() throws Exception {

        //传入true,默认使用客户端自带的观察者
        zooKeeper.getChildren("/data2",new Watcher() {

            // process由listener线程调用，listener线程不能阻塞,阻塞后无法再调用process
            //当前线程自己设置的观察者
            @Override
            public void process(WatchedEvent event) {

                System.out.println(event.getPath()+"发生了以下事件:"+event.getType());

                System.out.println(Thread.currentThread().getName()+"---->我还活着......");

                try {
                    //递归调用
                    lsAndAlwaysWatchCurrent();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

}
