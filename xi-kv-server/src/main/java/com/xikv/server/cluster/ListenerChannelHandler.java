package com.xikv.server.cluster;

import com.xikv.common.util.StringUtils;
import io.netty.channel.ChannelHandlerContext;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: ListenerChannelHandler
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
@io.netty.channel.ChannelHandler.Sharable
public class ListenerChannelHandler extends ChannelHandler {

    private LinkedBlockingQueue<Vote> recvQueue = new LinkedBlockingQueue<>();
    private Elector elector;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        //System.out.println("[ListenerChannelHandler] [channelActive].");
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg == null) {
            System.out.println("[ListenerChannelHandler] [channelRead] null = [" + StringUtils.getString(msg));
            return;
        }
        Vote vote = (Vote) msg;
        vote.setCtx(ctx);
        if (elector != null) {
            elector.resolve(vote);
        } else {
            recvQueue.add(vote);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("关闭连接：" + cause.getMessage());
        ctx.close();
    }

    @Override
    public Vote receiver(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return recvQueue.poll(timeout, timeUnit);
    }

    public void setElector(Elector elector) {
        this.elector = elector;
    }
}
