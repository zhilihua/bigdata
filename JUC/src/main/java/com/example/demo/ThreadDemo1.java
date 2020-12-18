package com.example.demo;

class Thread1 extends Thread {
    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            if (i%2==0){
                System.out.println(getName()+":------>"+i);
            }
        }
    }
}
//sdfsdfdsf
public class ThreadDemo1{
    public static void main(String[] args) {
        new Thread1().start();   //使用匿名类实现，继承Thead实现多线程，每次都需要创建对象

        for (int i = 0; i < 100; i++) {
            if (i%2 == 1) {
                System.out.println("main:------>"+i);
            }
        }
    }
}