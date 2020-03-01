package com.xikv.server;

import com.xikv.server.config.Configuration;
import com.xikv.server.memory.XiKVDatabase;
import com.xikv.server.network.ServerNet;
import com.xikv.server.network.ServerNetFactory;
import com.xikv.server.storage.StorageManager;

import java.io.File;
import java.io.IOException;

/**
 * @description: XiKVServer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiKVServer {

    public static void main(String[] args) {
        XiKVServer main = new XiKVServer();
        try {
            main.initializeAndRun(args);
        } catch (Exception e) {
            System.err.println(e);
            System.exit(2);
        }
        System.exit(0);
    }

    protected void initializeAndRun(String[] args) throws Exception {
        Configuration config = new Configuration();
        if (args.length == 1) {
            config.parse(args[0]);
        }
        runFromConfig(config);
    }

    private QuorumPeer quorumPeer;

    public void runFromConfig(Configuration config) throws IOException {
        System.out.println("Starting runFromConfig...");
        try {
            ServerNetFactory snf = ServerNetFactory.createServerNetFactory();
            snf.configure(config);
            quorumPeer = new QuorumPeer();
            quorumPeer.setConfiguration(config);
            quorumPeer.setServerNetFactory(snf);
            quorumPeer.setStorageManager(new StorageManager(config));
            quorumPeer.setXiDB(new XiKVDatabase(config));
            quorumPeer.start();
            quorumPeer.join();
        } catch (Exception e) {
            System.err.println("Quorum Peer interrupted " + e);
            e.printStackTrace();
        }
    }
}
