package com.jinfeng.spark;


import org.apache.log4j.Logger;

/**
 * Project : Spark
 * Package : com.jinfeng.spark
 * Author : WangJinfeng
 * Date : 2017-10-27 19:50
 * Email : wangjinfeng@yiche.com
 * Phone : 152-1062-7698
 */

public class HelloWorld {
    private static Logger logger = Logger.getLogger(HelloWorld.class);

    public static void main(String[] args) {
        System.out.println("Hello World!");
        logger.info("Hello World!");
    }
}
