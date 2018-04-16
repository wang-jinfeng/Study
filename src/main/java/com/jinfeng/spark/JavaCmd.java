package com.jinfeng.spark;

/**
 * Project: Study
 * Package: com.jinfeng.spark
 * Author: wangjf
 * Date: 2018/4/12
 * Email: wangjinfeng@reyun.com
 */

public class JavaCmd {
    public static void main(String[] args) {
        JavaCmd executor = new JavaCmd();
        //  executor.exec("1", "{\"id\":\"1\",\"tag\":[{\"_type\":true,\"_type_sql\":\"('020101','020102')\"},{\"_payment\":true,\"_payment_sql\":\"('0202','020201')\"}],\"os\":\"idfa\",\"relation\":true,\"start\":\"2017-08-03\",\"end\":\"2017-09-10\"}");
        executor.exec(args[0], args[1]);
    }

    public synchronized void exec(final String id, final String data) {
        new Thread() {
            public void run() {
                try {
                    System.out.println("id===>" + id + ",data===>" + data);
                    //  Runtime.getRuntime().exec("cd /home/ec2-user/dmp/wangjf && sh user_desk.sh " + id + " '" + data + "'");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }
}
