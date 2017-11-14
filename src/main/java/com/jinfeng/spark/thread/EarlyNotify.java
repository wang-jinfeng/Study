package com.jinfeng.spark.thread;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Project : Spark
 * Package : com.jinfeng.study.thread
 * Author : WangJinfeng
 * Date : 2017-09-15 18:14
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 */
public class EarlyNotify extends Object {
    private List list;

    public EarlyNotify() {
        list = Collections.synchronizedList(new ArrayList());
    }

    public String removeItem() throws InterruptedException {
        print("in removeItem() - entering");
        synchronized (list) {
            while (list.isEmpty()) {
                print("in removeItem() - about to wait()");
                list.wait();
                print("in removeItem() - done with wait()");
            }
            //  删除元素
            String item = (String) list.remove(0);
            print("in removeItem() - leaving");
            return item;
        }
    }

    public void addItem(String item) {
        print("in addItem() - entering");
        synchronized (list) {
            //  添加元素
            list.add(item);
            print("in addItem() - just added: '" + item + "'");

            //  添加后，通知所有线程
            list.notifyAll();
            print("in addItem() - just notified");
        }
        print("in addItem() - leaving");
    }

    private static void print(String msg) {
        String name = Thread.currentThread().getName();
        System.out.println(name + " : " + msg);
    }

    public static void main(String[] args) {
        final EarlyNotify en = new EarlyNotify();
        Runnable runA = new Runnable() {
            public void run() {
                try {
                    String item = en.removeItem();
                    print("in run() - returned: '" + item + "'");
                } catch (InterruptedException x) {
                    print("interrupted");
                } catch (Exception e) {
                    print("throw an Exception!!!\n" + e);
                }
            }
        };
        Runnable runB = new Runnable() {
            public void run() {
                en.addItem("Hello!");
            }
        };

        try {
            //  启动第一个删除元素的线程
            Thread threadA1 = new Thread(runA, "threadA1");
            threadA1.start();

            Thread.sleep(500);
            //  启动第二个删除元素的线程
            Thread threadA2 = new Thread(runA, "threadA2");
            threadA2.start();

            Thread.sleep(500);
            //  启动增加元素的线程
            Thread threadB = new Thread(runB, "threadB");
            threadB.start();
            Thread.sleep(1000);
            threadA1.interrupt();
            threadA2.interrupt();

        } catch (InterruptedException x) {
        }
    }
}
