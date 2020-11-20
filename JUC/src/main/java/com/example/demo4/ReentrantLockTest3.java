package com.example.demo4;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 案例 ： 饭店的配菜，炒菜，传菜进行交替执行

 */
class FD{

    private int state; //0表示配菜，1表示炒菜，2表示传菜

    //创建ReentrantLock
    private ReentrantLock lock = new ReentrantLock();
    //每一个condition控制一个线程的唤醒和等待
    private Condition peicai = lock.newCondition();
    private Condition chaocai = lock.newCondition();
    private Condition chuancai = lock.newCondition();

    public void peicai() {
        //加锁
        try {
            lock.lock();
            //业务逻辑
            if(state != 0) {//不需要配菜
                try {
                    peicai.await();//睡觉
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread().getName() + "====开始配菜====");
            //修改状态 - 让炒菜的哥们炒菜
            state = 1;
            //唤醒炒菜的哥们，炒菜
            chaocai.signal();
        } finally {
            lock.unlock();
        }
    }
    public void chaocai() {
        //加锁
        try {
            lock.lock();
            //业务逻辑
            if(state != 1) {//不需要炒菜
                try {
                    chaocai.await();//睡觉
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread().getName() + "====开始炒菜====");
            //修改状态 - 让传菜的哥们传菜
            state = 2;
            //唤醒传菜的哥们，传菜
            chuancai.signal();
        } finally {
            lock.unlock();
        }
    }
    public void chuancai() {
        //加锁
        try {
            lock.lock();
            //业务逻辑
            if(state != 2) {//不需要传菜
                try {
                    chuancai.await();//睡觉
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            System.out.println(Thread.currentThread().getName() + "====开始传菜====");
            //修改状态 - 让配菜的哥们配菜
            state = 0;
            //唤醒配菜的哥们，配菜
            peicai.signal();
        } finally {
            lock.unlock();
        }
    }
}


public class ReentrantLockTest3 {

    public static void main(String[] args) {

        FD fd = new FD();

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i <5; i++) {
                    fd.peicai();
                }
            }
        },"张三").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i <5; i++) {
                    fd.chaocai();
                }
            }
        },"李四").start();

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i <5; i++) {
                    fd.chuancai();
                }
            }
        },"王五").start();
    }
}

















