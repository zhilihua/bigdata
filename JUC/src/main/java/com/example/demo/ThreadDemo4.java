package com.example.demo;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;

class MyCallable implements Callable<Integer>{
    @Override
    public Integer call() throws Exception {
        int num=0;
        for (; num < 50; num++) {
            System.out.println(Thread.currentThread().getName()+"======》"+num);

        }
        return num;
    }
}

public class ThreadDemo4 {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        MyCallable myCallable = new MyCallable();
        FutureTask<Integer> futureTask = new FutureTask<>(myCallable);
        Thread thread = new Thread(futureTask);
        thread.start();

        Integer integer = futureTask.get();
        System.out.println(integer);
        System.out.println("主线程结束！");
    }
}
