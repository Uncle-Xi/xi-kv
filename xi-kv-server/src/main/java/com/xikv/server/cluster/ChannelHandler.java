package com.xikv.server.cluster;

import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.TimeUnit;

public abstract class ChannelHandler extends ChannelInboundHandlerAdapter {

    public abstract Vote receiver(long timeout, TimeUnit timeUnit) throws InterruptedException;
}
