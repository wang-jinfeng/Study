package com.jinfeng.spark;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;

import java.io.File;
import java.io.IOException;

/**
 * Project: UserDesk
 * Package: com.reyun.bpu.tools
 * Author: wangjf
 * Date: 2018/3/27
 * Email: wangjinfeng@reyun.com
 */

class SSHCommand {
    private Connection connection;
    private String hostaddr;
    private String username;
    private File pemFile;
    private String password;

    public SSHCommand(String hostaddr, String username, File pemFile, String password) {
        this.hostaddr = hostaddr;
        this.username = username;
        this.pemFile = pemFile;
        this.password = password;
    }

    public boolean login() throws IOException {
        connection = new Connection(hostaddr);
        connection.connect();
        return connection.authenticateWithPublicKey(username, pemFile, password);
    }

    public synchronized int exec(final String id, final String data) {
        int state = 0;
        try {
            if (this.login()) {
                final Session session = connection.openSession();
                state = session.getState();
                new Thread() {
                    public void run() {
                        try {
                            session.execCommand("cd /home/ec2-user/dmp/wangjf && sh user_desk.sh " + id + " '" + data + "'");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        session.close();
                        connection.close();
                    }
                }.start();
            }
        } catch (IOException e) {
            e.printStackTrace();
            connection.close();
        }
        return state;
    }
}


public class SSHCommandExecutor {
    public static void main(String[] args) throws IOException {
        SSHCommand executor = new SSHCommand("52.80.60.229", "ec2-user",
                new File("/Users/wangjf/WorkSpace/bpu-pem/bpu-test-new.pem"), null);
        int result = executor.exec("1", "{\"id\":\"1\",\"tag\":[{\"_type\":true,\"_type_sql\":\"('020101','020102')\"},{\"_payment\":true,\"_payment_sql\":\"('0202','020201')\"}],\"os\":\"idfa\",\"relation\":true,\"start\":\"2017-08-03\",\"end\":\"2017-09-10\"}");

        System.out.println("Login ==> " + result);
    }
}