package com.xikv.server;

import com.xikv.common.thread.XiKVThread;
import com.xikv.server.cluster.*;
import com.xikv.server.config.Configuration;
import com.xikv.server.memory.XiKVDatabase;
import com.xikv.server.network.ServerNet;
import com.xikv.server.network.ServerNetFactory;
import com.xikv.server.storage.StorageManager;
import io.netty.channel.ChannelHandler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static com.xikv.server.SevState.*;

/**
 * @description: QuorumPeer
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class QuorumPeer extends XiKVThread {

    protected ServerNetFactory serverNetFactory;
    protected StorageManager storageManager;
    protected XiKVDatabase xiDB;
    public Configuration configuration;
    private SevState state = FOLLOWING;
    volatile boolean running = true;
    protected RequestProcessor firstProcessor;

    public QuorumPeer() {
        super("QuorumPeer");
    }

    @Override
    public synchronized void start() {
        loadDataBase();
        serverNetFactory.start();
        serverNetFactory.setPeer(this);
        super.start();
    }

    private void loadDataBase() {
        storageManager.setXiDB(xiDB);
        storageManager.loadData();
    }

    ListenerChannelHandler channelHandler = new ListenerChannelHandler();
    Contacter contacter = new Contacter();
    Candidate election;
    Follower follower;
    Leader leader;

    @Override
    public void run() {
        System.out.println("Starting quorum peer");
        startListener();
        try {
            while (running) {
                switch (getSevState()) {
                    case LOOKING:
                        System.out.println("[QuorumPeer] LOOKING");
                        try {
                            System.out.println("[QuorumPeer] Starting lookForLeader...");
                            election = new Candidate(this);
                            election.leaderSelector();
                        } catch (Exception e) {
                            System.err.println("[QuorumPeer] Unexpected exception" + e);
                            setSevState(SevState.LOOKING);
                            e.printStackTrace();
                        }
                        break;
                    case FOLLOWING:
                        try {
                            System.out.println("[QuorumPeer] FOLLOWING");
                            follower = new Follower(this);
                            follower.follow();
                        } catch (Exception e) {
                            System.err.println("[QuorumPeer] Unexpected exception" + e);
                        } finally {
                            follower = null;
                            setSevState(SevState.LOOKING);
                        }
                        break;
                    case LEADING:
                        try {
                            //CountDownLatch count = new CountDownLatch(1);
                            //count.await();
                            System.out.println("[QuorumPeer] LEADING");
                            leader = new Leader(this);
                            leader.lead();
                        } catch (Exception e) {
                            System.out.println("[QuorumPeer] Unexpected exception" + e);
                        } finally {
                            leader = null;
                            setSevState(SevState.LOOKING);
                        }
                        break;
                }
            }
        } finally {
            System.out.println("QuorumPeer main thread exited");
        }
    }

    private void startListener(){
        int serverId = configuration.getServerId();
        Configuration.Server server = configuration.getServers().get(serverId);
        contacter.startListener(server.getPort(), channelHandler);
    }

    public void processConnectRequest(ServerNet net) throws IOException {
        try {
            if (firstProcessor == null) {
                System.out.println("集群尚未初始化...");
                return;
            }
            firstProcessor.processRequest(net);
        } catch (RequestProcessor.RequestProcessorException e) {
            System.out.println("Unable to process request:" + e.getMessage());
            e.printStackTrace();
        }
    }

    public ServerNetFactory getServerNetFactory() {
        return serverNetFactory;
    }

    public void setServerNetFactory(ServerNetFactory serverNetFactory) {
        this.serverNetFactory = serverNetFactory;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }

    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    public XiKVDatabase getXiDB() {
        return xiDB;
    }

    public void setXiDB(XiKVDatabase xiDB) {
        this.xiDB = xiDB;
    }

    public synchronized SevState getSevState() {
        if ("standalone".equalsIgnoreCase(configuration.getStartModel())) {
            System.out.println("默认以单机启动...");
            state = LEADING;
        }
        return state;
    }

    public void setSevState(SevState state) {
        this.state = state;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setFirstProcessor(RequestProcessor firstProcessor) {
        this.firstProcessor = firstProcessor;
    }

    public ListenerChannelHandler getChannelHandler() {
        return channelHandler;
    }
}
