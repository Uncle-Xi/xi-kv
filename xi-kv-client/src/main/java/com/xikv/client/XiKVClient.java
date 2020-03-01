package com.xikv.client;

import com.xikv.XiKV;
import com.xikv.common.thread.XiKVThread;
import com.xikv.common.util.StringUtils;
import com.xikv.server.Request;
import com.xikv.server.Response;
import com.xikv.server.cli.controller.CommandController;
import com.xikv.server.network.XiDecoder;
import com.xikv.server.network.XiEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: XiKVClient
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiKVClient {

    private ClientHandler clientHandler = new ClientHandler();
    private SendThread sendThread;
    private XiKV kv;
    private ConnectStringParser connectStringParser;
    private final LinkedBlockingQueue<Request> sendQueue = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<Response> resultQueue = new LinkedBlockingQueue<>();
    private ArrayList<InetSocketAddress> serverAddresses = new ArrayList<InetSocketAddress>();

    public XiKVClient(String connectString, XiKV kv) {
        this.kv = kv;
        this.sendThread = new SendThread(clientHandler);
        this.connectStringParser = new ConnectStringParser(connectString);
        this.serverAddresses = connectStringParser.getServerAddresses();
        this.connect(next(0));
    }

    public Response submitRequest(Request request) throws InterruptedException {
        //System.out.println("submitRequest ... " + StringUtils.getString(request));
        if (sendQueue.add(request)) {
            return resultQueue.take();
        }
        System.out.println("没有拿到同步结果...");
        return null;
    }

    class ClientHandler extends ChannelInboundHandlerAdapter {
        private ChannelHandlerContext ctx;

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            this.ctx = ctx;
            //System.out.println("channelActive ... ");
        }

        private boolean sendMsg(Request msg) throws InterruptedException {
            while (ctx == null) {
                Thread.sleep(200);
            }
            ctx.channel().writeAndFlush(StringUtils.getString(msg));
            return true;
        }

        @Override
        public void channelRead(ChannelHandlerContext channelHandlerContext, Object object) throws Exception {
            try {
                resultQueue.add((Response) StringUtils.getObjectByClazz((String) object, Response.class));
            } catch (Exception e) {
                System.out.println("转换错误");
                e.printStackTrace();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.out.println("与服务器断开连接:" + cause.getMessage());
            ctx.close();
        }
    }

    class SendThread extends XiKVThread {
        ClientHandler handler;

        public SendThread(ClientHandler handler) {
            super("SendThread...");
            this.handler = handler;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    Request request = sendQueue.take();
                    handler.sendMsg(request);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void connect(InetSocketAddress address) {
        new Thread(() -> {
            EventLoopGroup workerGroup = new NioEventLoopGroup();
            try {
                Bootstrap b = new Bootstrap();
                b.group(workerGroup);
                b.channel(NioSocketChannel.class);
                b.option(ChannelOption.SO_KEEPALIVE, true);
                b.handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(65535, 0, 2, 0, 2));
                        pipeline.addLast(new XiDecoder());
                        pipeline.addLast(new LengthFieldPrepender(2));
                        pipeline.addLast(new XiEncoder());
                        pipeline.addLast(clientHandler);
                    }
                });
                ChannelFuture f = b.connect(address).sync();
                f.channel().closeFuture().sync();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                workerGroup.shutdownGracefully();
            }
        }).start();
    }

    public void start() {
        sendThread.start();
    }

    private int lastIndex = -1;
    private int currentIndex = -1;

    public InetSocketAddress next(long spinDelay) {
        currentIndex = ++currentIndex % serverAddresses.size();
        if (currentIndex == lastIndex && spinDelay > 0) {
            try {
                Thread.sleep(spinDelay);
            } catch (InterruptedException e) {
                System.out.println("Unexpected exception" + e);
            }
        } else if (lastIndex == -1) {
            lastIndex = 0;
        }
        return serverAddresses.get(currentIndex);
    }
}
