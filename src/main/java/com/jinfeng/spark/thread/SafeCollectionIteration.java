package com.jinfeng.spark.thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Project : Spark
 * Package : com.jinfeng.study.thread
 * Author : WangJinfeng
 * Date : 2017-09-15 14:43
 * Email : jinfeng.wang@yoyi.com.cn
 * Phone : 152-1062-7698
 */
public class SafeCollectionIteration {
    public static void main(String[] args) {
        //  为了安全起见，仅使用同步列表的一个引用，这样可以确保控制了所有访问
        //  集合必须同步化，这里是一个List
        List wordList = Collections.synchronizedList(new ArrayList());
        //  wordList中的add方法是同步方法，会获取wordList实例的对象锁
        wordList.add("Iterators");
        wordList.add("require");
        wordList.add("special");
        wordList.add("handing");

        //  获取wordList实例的对象锁
        //  迭代时，阻塞其他线程调用add或remove等方法修改元素
        synchronized (wordList) {
            Iterator iter = wordList.iterator();
            while (iter.hasNext()) {
                String s = (String) iter.next();
                System.out.println("found string:" + s + ",length = " + s.length());
            }
        }
    }
}
