package com.example.demo2;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
    Executors.newCachedThreadPool()： 创建一个可缓存线程池，如果线程池长度超过处理需要，可灵活回收空闲线程，若无可回收，则新建线程。
	 Executors.newFixedThreadPool(n); 创建一个可定长线程数的线程池，可控制最大并发数，超出的线程会在队列中等待分配。
	 Executors.newSingleThreadExecutor() ：创建一个只有一个线程的线程池，单一线程可以保证所有的任务按照指定的顺序执行。
 	 Executors.newScheduledThreadPool(n)：创建一个线程池，它可安排在给定延迟后运行命令或者定期地执行。

 */
public class ThreadPool {
    public static void main(String[] args) {
        //创建线程池
        ExecutorService pool = Executors.newCachedThreadPool();
        //执行任务
        pool.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    System.out.println(Thread.currentThread().getName() + "====" + i);
                }
            }
        });

        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName() + "====" + i);
        }

        pool.shutdown();
    }
}
