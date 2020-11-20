package com.example.demo;

class Thread2 implements Runnable{

    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            if (i%2 == 0) {
                System.out.println(Thread.currentThread().getName()+":------>"+i);
            }
        }
    }
}

public class ThreadDemo2 {
    public static void main(String[] args) {
        Runnable r =new Thread2();  //创建多个线程只需要创建一次对象即可

        new Thread(r).start();

        for (int i = 0; i < 100; i++) {
            if (i%2==1) {
                System.out.println("main:------>"+i);
            }
        }
    }
}
