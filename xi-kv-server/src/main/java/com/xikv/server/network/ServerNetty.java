package com.xikv.server.network;

import com.xikv.common.util.StringUtils;
import com.xikv.server.QuorumPeer;
import com.xikv.server.Record;
import com.xikv.server.Request;
import com.xikv.server.config.Configuration;
import com.xikv.server.memory.XiKVDatabase;
import com.xikv.server.storage.StorageManager;
import io.netty.channel.ChannelHandlerContext;

import java.io.IOException;

/**
 * @description: ServerNetty
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ServerNetty extends ServerNet{

    QuorumPeer peer;
    NettyServerFactory factory;
    ChannelHandlerContext ctx;

    ServerNetty(ChannelHandlerContext ctx, QuorumPeer peer, NettyServerFactory factory) {
        this.ctx = ctx;
        this.peer = peer;
        this.factory = factory;
    }

    public void receiveMessage() {
        try {
            peer.processConnectRequest(this);
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void sendResponse(Record record) throws IOException {
        ctx.writeAndFlush(StringUtils.getString(record));
    }
}
