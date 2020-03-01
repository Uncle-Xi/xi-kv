package com.xikv.server.network;

import com.xikv.common.util.StringUtils;
import com.xikv.server.QuorumPeer;
import com.xikv.server.Request;
import com.xikv.server.cli.controller.CommandController;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @description: ServerNetFactory
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class NettyServerFactory extends ServerNetFactory {

    ServerBootstrap bootstrap = new ServerBootstrap();
    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    InetSocketAddress localAddress;

    QuorumPeer peer;
    NettyServerFactory factory;

    class NettyServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //System.out.println("NettyServerHandler channelActive...");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //if (msg instanceof FullHttpRequest) {
            //    System.out.println("http");
            //    new CommandController().channelRead0(ctx, (FullHttpRequest) msg);
            //} else {
            //
            //}
            //System.out.println("NettyServerFactory channelRead -> " + msg);
            ServerNetty server = new ServerNetty(ctx, peer, factory);
            server.setRequest((Request)StringUtils.getObjectByClazz((String) msg, Request.class));
            synchronized (server) {
                processMessage(server);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("关闭连接：" + cause.getMessage());
            ctx.close();
        }

        private void processMessage(ServerNetty server) {
            server.receiveMessage();
        }
    }

    class ProtocolDispatcher extends ByteToMessageDecoder {

        @Override
        public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
            if (in.readableBytes() < 5) {
                return;
            }
            int readerIndex = in.readerIndex();
            final int magic1 = in.getByte(readerIndex);
            final int magic2 = in.getByte(readerIndex + 1);
            if (isHttp(magic1, magic2)) {
                dispatchToHttp(ctx);
            } else {
                dispatchToPacket(ctx);
            }
        }

        private boolean isHttp(int magic1, int magic2) {
            return  magic1 == 'G' && magic2 == 'E' || // GET
                    magic1 == 'P' && magic2 == 'O' || // POST
                    magic1 == 'P' && magic2 == 'U' || // PUT
                    magic1 == 'H' && magic2 == 'E' || // HEAD
                    magic1 == 'O' && magic2 == 'P' || // OPTIONS
                    magic1 == 'P' && magic2 == 'A' || // PATCH
                    magic1 == 'D' && magic2 == 'E' || // DELETE
                    magic1 == 'T' && magic2 == 'R' || // TRACE
                    magic1 == 'C' && magic2 == 'O';   // CONNECT
        }

        private void dispatchToPacket(ChannelHandlerContext ctx) {
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
            pipeline.addLast(new XiDecoder());
            pipeline.addLast(new LengthFieldPrepender(2));
            pipeline.addLast(new XiEncoder());
            pipeline.addLast(new NettyServerHandler());
            pipeline.remove(this);
            ctx.fireChannelActive();
        }

        private void dispatchToHttp(ChannelHandlerContext ctx) {
            ChannelPipeline pipeline = ctx.pipeline();
            pipeline.addLast(new HttpServerCodec());
            pipeline.addLast(new HttpObjectAggregator(64 * 1024));
            pipeline.addLast(new ChunkedWriteHandler());
            pipeline.addLast(new CommandController(peer, factory));
            pipeline.remove(this);
            ctx.fireChannelActive();
        }
    }


    NettyServerFactory() {
        this.factory = this;
        bootstrap.group(bossGroup, workerGroup);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel ch) throws Exception {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast(new ProtocolDispatcher());
//                pipeline.addLast(new HttpServerCodec());
//                pipeline.addLast(new HttpObjectAggregator(64 * 1024));
//                pipeline.addLast(new ChunkedWriteHandler());
//                pipeline.addLast(new CommandController());
//
//                pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
//                pipeline.addLast(new XiDecoder());
//                pipeline.addLast(new LengthFieldPrepender(2));
//                pipeline.addLast(new XiEncoder());
//                pipeline.addLast(new NettyServerHandler());
            }
        });
        bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
        bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
    }

    @Override
    public synchronized void start() {
        this.localAddress = new InetSocketAddress(configuration.getPort());
        new Thread(() -> {
            try {
                System.out.println("binding to port " + localAddress);
                ChannelFuture channelFuture = bootstrap.bind(localAddress).sync();
                channelFuture.channel().closeFuture().sync();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                bossGroup.shutdownGracefully();
                workerGroup.shutdownGracefully();
            }
        }).start();
    }

    @Override
    public void setPeer(QuorumPeer peer) {
        this.peer = peer;
    }
}
