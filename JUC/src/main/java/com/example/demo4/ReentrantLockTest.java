package com.example.demo4;

import java.util.concurrent.locks.ReentrantLock;

/**
 ReentrantLock ：

 说明：除了同步代码块和同步方法外。JUC推荐我们使用ReentrantLock来解决线程同步的问题。

 理由 ：使用ReentrantLock更灵活。（线程通信 ）

 使用：
 1.创建ReentrantLock的对象
 2.使用lock()上锁
 3.在finally中使用unlock()解锁
 */
public class ReentrantLockTest {
    public static void main(String[] args) {
        MyRunnable mr = new MyRunnable();
        new Thread(mr, "窗口1").start();
        new Thread(mr, "窗口2").start();
    }
}

class MyRunnable implements Runnable{

    private int ticket = 10000;
    //创建ReentrantLock的对象
    //构造器 ：ReentrantLock(boolean fair) 值为true的时候会创建一个公平策略（公平锁） - 消耗性能为代价
    private ReentrantLock lock = new ReentrantLock(true);

    @Override
    public void run() {
        while (true){
            try {
                lock.lock(); //使用方法进行加锁
                if(ticket > 0) {
                    System.out.println(Thread.currentThread().getName() + "===" + ticket);
                    ticket--;
                }else {
                    return;
                }
            } finally {
                lock.unlock();//使用方法解锁
            }

        }
    }
}
