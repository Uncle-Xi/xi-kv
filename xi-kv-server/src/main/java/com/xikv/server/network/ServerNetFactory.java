package com.xikv.server.network;

import com.xikv.common.thread.XiKVThread;
import com.xikv.server.QuorumPeer;
import com.xikv.server.config.Configuration;

/**
 * @description: ServerNetFactory
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class ServerNetFactory {

    Configuration configuration;

    public void configure(Configuration configuration){
        this.configuration = configuration;
    }

    public static ServerNetFactory createServerNetFactory(){
        return new NettyServerFactory();
    }

    public abstract void setPeer(QuorumPeer peer);

    public abstract void start();
}
