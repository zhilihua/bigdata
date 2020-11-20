package com.example.demo4;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 使用ReentrantLock实现线程通信

 说明：
 1.如果要通过ReentrantLock实现线程通信，那么必须使用Condition（是通过ReentrantLock创建的）
 2.notify唤醒是随机唤醒使用condition可以精确唤醒
 3.signal() 线程唤醒
 4.await() 线程等待
 */
public class ReentrantLockTest2 {

    public static void main(String[] args) {
        MyRun mr = new MyRun();
        new Thread(mr,"线程1-------").start();
        new Thread(mr,"线程2=======").start();
    }
}

class MyRun implements Runnable{

    private ReentrantLock lock = new ReentrantLock();
    //如果要通过ReentrantLock实现线程通信，那么必须使用Condition（是通过ReentrantLock创建的）
    private Condition condition = lock.newCondition();

    private int ticket = 100;

    @Override
    public void run() {
        while(true) {
            try {
                lock.lock();
                /*
                 * notify唤醒是随机唤醒
                 * 使用condition可以精确唤醒
                 */
                condition.signal();//线程唤醒

                if(ticket > 0) {
                    System.out.println(Thread.currentThread().getName() + "====" + ticket);
                    ticket--;
                }else {
                    return;
                }

                try {
                    condition.await(); //线程等待
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }finally {
                lock.unlock();
            }
        }
    }

}

