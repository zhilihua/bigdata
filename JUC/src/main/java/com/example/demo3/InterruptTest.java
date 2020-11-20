package com.example.demo3;

/**
 * 终止线程 ：
 * <p>
 * interrupt : 打断线程（设置标记）
 * 注意 ：标记有时会设置失败。比如当前前线程被IO阻塞，或者分线程调用sleep等操作。
 * 描述 ：如果线程在wait()，wait(long)调用阻塞，或wait(long, int)
 * Object类的方法，或者对join()，join(long)，join(long, int)，sleep(long)，或sleep(long, int)，
 * 这类方法，那么它的中断状态将被清除，它会收到一InterruptedException。
 * <p>
 * isInterrupted : 当前线程是否已经被杀死（根据标记来判断）
 */
public class InterruptTest {
    public static void main(String[] args) {
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < 10; i++) {
                    System.out.println(Thread.currentThread().getName() + " ==="
                            + Thread.currentThread().isInterrupted() + "===" + i);

                    try {
                        //注意:interrupt标记失败。线程不会终止
                        Thread.currentThread().sleep(1000);
                    } catch (InterruptedException e) {
//                        e.printStackTrace();
                        //进行为完成的工作
                        System.out.println("数据保存中.........");
                        //结束当前方法
                        return;
                    }
                    //判断是否已经被中断
                    if(Thread.currentThread().isInterrupted()){
                        //进行为完成的工作
                        System.out.println("数据保存中.........");
                        //结束当前方法
                        return;
                    }
                }
            }
        }, "线程1=========");

        t.start();

        //主线程循环
        for (int i = 0; i < 10; i++) {
            System.out.println(Thread.currentThread().getName() + " ===" + i);
            if (i == 3) {
                t.interrupt();
            }
        }
    }
}
