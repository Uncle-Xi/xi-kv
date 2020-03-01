package com.demo.xi.kv;


import com.xikv.XiKV;

/**
 * @description: employ xikv
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SimpleUse {

    private static final String CONNECT_ADDRS = "127.0.0.1:6379";
    private static final String PASSWORD = "password";
    private static final String KEY_PREFFIX = "key-hello-";

    public static void main(String[] args) throws InterruptedException {
        XiKV xiKV = new XiKV(CONNECT_ADDRS, PASSWORD);
        for (int i = 0; i < 1000; i++) {
            xiKV.set(KEY_PREFFIX + i, KEY_PREFFIX + i, 7 * 24 * 60 * 60 * 1000);
        }
        for (int i = 0; i < 1000; i++) {
            System.out.println(xiKV.get(KEY_PREFFIX + i));
        }
        System.out.println(xiKV.size(KEY_PREFFIX));
    }
}
