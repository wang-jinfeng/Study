package com.jinfeng.spark.thread;

/**
 * Project : Spark
 * Package : com.jinfeng.study.thread
 * Author : WangJinfeng
 * Date : 2017-09-15 15:05
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 */
public class DeadLock extends Object {
    private String objID;

    public DeadLock(String id) {
        objID = id;
    }

    public synchronized void checkOther(DeadLock other) {
        print("entering checkOther()");
        try {
            Thread.sleep(2000);
        } catch (InterruptedException x) {
        }
        print("in checkOther() - about to " + "invoke 'other.action()'");
        //  调用other对象的action方法，由于该方法是同步方法，因此会试图获取other对象的对象锁
        other.action();
        print("leaving checkOther()");
    }

    public synchronized void action() {
        print("entering action()");
        try {
            Thread.sleep(500);
        } catch (InterruptedException x) {
        }
        print("leaving action()");
    }

    public void print(String msg) {
        threadPrint("objID = " + objID + " - " + msg);
    }

    public static void threadPrint(String msg) {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " : " + msg);
    }

    public static void main(String[] args) {
        final DeadLock obj1 = new DeadLock("obj1");
        final DeadLock obj2 = new DeadLock("obj2");
        Runnable runA = new Runnable() {
            public void run() {
                obj1.checkOther(obj2);
            }
        };

        Thread threadA = new Thread(runA, "threadA");
        threadA.start();

        try {
            Thread.sleep(200);
        } catch (InterruptedException x) {
        }

        Runnable runB = new Runnable() {
            public void run() {
                obj2.checkOther(obj1);
            }
        };
        Thread threadB = new Thread(runB, "threadB");
        threadB.start();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException x) {
        }

        threadPrint("finished sleeping");

        threadPrint("about to interrupt() threadA");
        threadA.interrupt();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException x) {
        }

        threadPrint("about to interrupt() threadB");
        threadB.interrupt();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException x) {
        }

        threadPrint("did that break the deadlock?");
    }
}
