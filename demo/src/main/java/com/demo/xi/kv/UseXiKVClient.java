package com.demo.xi.kv;


import com.xikv.XiKV;

/**
 * @description: employ xikv
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class UseXiKVClient {

    public static void main(String[] args) throws InterruptedException {
        XiKV xi = new XiKV("127.0.0.1:6379", "password");
        getAndSet(xi);
    }

    public static void get(XiKV xi) throws InterruptedException {
        xi.set("hello-" + 0, "setValueid=0", 1000000);
        System.out.println(xi.get("hello-" + 0));
    }

    public static void getAndSet(XiKV xi) throws InterruptedException {
        for (int i = 0; i < 130; i++) {
            xi.set("hello-" + i, "xiKv-" + i, 10000000);
        }
        for (int i = 0; i < 100; i++) {
            System.out.println(xi.get("hello-" + i));
        }
        System.out.println(xi.size("hello-"));
    }
}
