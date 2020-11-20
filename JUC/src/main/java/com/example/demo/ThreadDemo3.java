package com.example.demo;

public class ThreadDemo3 {
    public static void main(String[] args) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 100; i++) {
                    if (i % 2 == 0) {
                        System.out.println(Thread.currentThread().getName() + ":------>" + i);
                    }
                }
            }
        }).start();
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 1) {
                System.out.println("main:------>" + i);
            }
        }
    }
}
