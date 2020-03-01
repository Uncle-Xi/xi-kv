package com.xikv.server.cluster;

import com.xikv.common.util.StringUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * @description: send vote
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SendVote {

    public static void sendOneWay(InetSocketAddress addr, Object data) throws IOException {
        ObjectOutputStream out = null;
        try {
            Socket socket = new Socket(addr.getHostString(), addr.getPort());
            out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(data);
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (out != null) { out.close(); }
        }
    }

    public static Object sendByIo(InetSocketAddress addr, Object data) throws IOException {
        System.out.println("[SendVote] [sendByIo] start.");
        ObjectOutputStream out = null;
        ObjectInputStream ois = null;
        //ObjectInputStream oin = null;
        try {
            Socket socket = new Socket(addr.getHostString(), addr.getPort());
            out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(data);
            out.writeObject(null);
            out.flush();
            //byte[] buff = new byte[1024];
            //int len = 0;
            //if ((len = socket.getInputStream().read(buff)) > 0) {
            //    in = new ByteArrayInputStream(buff, 0, len);
            //    in.read(buff, 0, len);
            //}
            ois = new ObjectInputStream(socket.getInputStream());
            //oin = new ObjectInputStream(socket.getInputStream());
            return ois.readObject();
            //return new ObjectInputStream(in).readObject();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ois != null) {
                ois.close();
            }
            if (out != null) {
                out.close();
            }
        }
        return null;
    }

    public static Object send(InetSocketAddress addr, Object data) {
        /**System.out.println("[SendVote] [send] start.");*/
        EventLoopGroup group = new NioEventLoopGroup();
        SendHandler sendHandler = new SendHandler();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group)
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ChannelPipeline pipeline = ch.pipeline();
                            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
                            pipeline.addLast("encoder", new ObjectEncoder());
                            pipeline.addLast("decoder", new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
                            pipeline.addLast("handler", sendHandler);
                        }
                    });

            ChannelFuture future = bootstrap.connect(addr).sync();
            future.channel().writeAndFlush(data).sync();
            future.channel().closeFuture().sync();
        } catch (Exception e) {
            //e.printStackTrace();
            throw new RuntimeException("[连接失败][拒绝连接]");
        } finally {
            group.shutdownGracefully();
        }
        /**System.out.println("[SendVote][等到了结果].");*/
        return sendHandler.getResponse();
    }

    @io.netty.channel.ChannelHandler.Sharable
    static class SendHandler extends ChannelInboundHandlerAdapter {
        private Object response;
        public Object getResponse() {
            return response;
        }
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //System.out.println("[SendHandler] [channelActive].");
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            //System.out.println("[SendHandler] [channelRead] " + StringUtils.getString(msg));
            response = msg;
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
