package com.jinfeng.spark.thread;

/**
 * Project : Spark
 * Package : com.jinfeng.study.examples
 * Author : WangJinfeng
 * Date : 2017-09-13 18:11
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 */

class MyThread extends Thread {
    private int ticket = 5;

    public void run(){
        for (int i = 0; i < 10; i++) {
            if (ticket > 0) {
                System.out.println("ticket = " + ticket--);
            }
        }
    }
}

public class ThreadDemo {
    public static void main(String[] args) {
        new MyThread().start();
        new MyThread().start();
        new MyThread().start();
    }
}