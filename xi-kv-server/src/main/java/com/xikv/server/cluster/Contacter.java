package com.xikv.server.cluster;

import com.xikv.common.thread.XiKVThread;
import com.xikv.common.util.StringUtils;
import com.xikv.server.QuorumPeer;
import com.xikv.server.SevState;
import com.xikv.server.config.Configuration;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: CollectVotes
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Contacter /*extends XiKVThread*/ {

    public void startListener(int port, ChannelHandler channelHandler) {
        ServerBootstrap bootstrap = new ServerBootstrap();
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        InetSocketAddress localAddress = new InetSocketAddress(port);
        new Thread(() -> {
            try {
                bootstrap.group(bossGroup, workerGroup);
                bootstrap.channel(NioServerSocketChannel.class);
                bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
                        pipeline.addLast(new LengthFieldPrepender(4));
                        pipeline.addLast("encoder", new ObjectEncoder());
                        pipeline.addLast("decoder", new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
                        pipeline.addLast(channelHandler);
                    }
                });
                bootstrap.option(ChannelOption.SO_REUSEADDR, true);
                bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
                bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
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











//    volatile boolean shutdown = false;
//    QuorumPeer self;
//    Configuration.Server server;
//    volatile ServerSocket serverSocket;
//    Map<Integer, Configuration.Server> serverMap;
//    private int myId;
//    Candidate election;
//    SendVoteWorker sendVoteWorker = null;
//
//    public Contacter(QuorumPeer self, Candidate election) {
//        super("Contacter");
//        this.election = election;
//        this.self = self;
//        this.serverMap = self.configuration.getServers();
//        this.myId = self.configuration.getServerId();
//        this.server = serverMap.get(myId);
//    }
//
//    public void starter() {
//        sendVoteWorker = new SendVoteWorker();
//        sendVoteWorker.start();
//        super.start();
//    }

//    @Override
//    public void run() {
//        try {
//            listener();
//            System.out.println("Contacter 启动在 -> " + serverSocket + " - " + this.server.getPort());
//            serverSocket = new ServerSocket(this.server.getPort());
//            while (!shutdown) {
//                try {
//                    Socket client = serverSocket.accept();
//                    receivePacket(client);
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

//    Map<Integer, Boolean> pollMap = new HashMap<>();

//    private void receivePacket(Socket client) {
//        InputStream in = null;
//        OutputStream out = null;
//        try {
//            in = client.getInputStream();
//            out = client.getOutputStream();
//            String content = "";
//            byte[] buff = new byte[1024];
//            int len;
//            if ((len = in.read(buff)) > 0) {
//                content += new String(buff, 0, len);
//            }
//            System.out.printf("receivePacket -> client = [%s], Vote = [%s]\n", client, content);
//            if (StringUtils.isEmpty(content)) {
//                return;
//            }
//            Vote vote = (Vote) StringUtils.getObjectByClazz(content, Vote.class);
//            //vote.setSign("数据回写成功！");
//            //sendMsg(vote, self.configuration.getServers().get(vote.getServerId()));
//            if (vote.getServerId() == myId) {
//                if (vote.getElectionCode() == ElectionCode.Agree
//                        && election.getVote().getEpoch() == vote.getEpoch()) {
//                    recvQueue.add(true);
//                    System.out.println("我的投票请求，别人通过了；本次通信结束。");
//                    return;
//                }
//                recvQueue.add(false);
//                updateEpoch(vote);
//                System.out.println("别人的回应[更新epoch后]就此打住。");
//                return;
//            } else {
//                if (vote.getElectionCode() == ElectionCode.canvass
//                        && election.getVote().getEpoch() == vote.getEpoch()
//                        && pollMap.get(election.getVote().getEpoch()) == null) {
//                    System.out.println("对方请求投票 - 我未投票|选举朝代一致【给他】 vote.epoch = " + vote.getEpoch());
//                    pollMap.put(election.getVote().getEpoch(), true);
//                    vote.setElectionCode(ElectionCode.Agree);
//                    out.write(StringUtils.getString(vote).getBytes("UTF-8"));
//                    out.flush();
//                    //PrintWriter pw = new PrintWriter(out);
//                    //pw.println(StringUtils.getString(vote));
//                    //pw.flush();
//                    return;
//                }
//                if (vote.getElectionCode() == ElectionCode.landlord) {
//                    self.setSevState(SevState.FOLLOWING);
//                    System.out.println("其他节点选举成功，直接跟随，数据原封不动丢回去！");
//                } else {
//                    vote.setElectionCode(ElectionCode.refuse);
//                    System.out.println("其他情况，全部拒绝 - " + StringUtils.getString(vote));
//                }
//                updateEpoch(vote);
//                out.write((StringUtils.getString(vote)).getBytes("UTF-8"));
//                out.flush();
//                //vote.setSign("数据回写成功！");
//                //PrintWriter pw = new PrintWriter(out);
//                //pw.println(StringUtils.getString(vote));
//                //pw.flush();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (out != null) {
//                    out.close();
//                }
//                if (in != null) {
//                    in.close();
//                }
//                client.close();
//            } catch (IOException ex) {
//                ex.printStackTrace();
//            }
//        }
//    }


//    protected void sendMsg(Vote vote, Configuration.Server server){
//        try {
//            ServerSocket s = new ServerSocket(0);
//            System.out.println("listening on port: " + s.getLocalPort());
//            DatagramSocket client = new DatagramSocket(s.getLocalPort());
//            byte [] data = StringUtils.getString(vote).getBytes();
//            DatagramPacket packet =
//                    new DatagramPacket(data, data.length,
//                            new InetSocketAddress(server.getIp(), server.getPort()));
//            client.send(packet);
//            client.close();
//            System.out.println("数据发送完后。");
//        } catch (Exception e){
//            e.printStackTrace();
//        }
//    }

    /**********************************************************************************/

//    public void listener(){
//        ServerBootstrap bootstrap = new ServerBootstrap();
//        EventLoopGroup bossGroup = new NioEventLoopGroup();
//        EventLoopGroup workerGroup = new NioEventLoopGroup();
//        InetSocketAddress localAddress = new InetSocketAddress(this.server.getPort());
//        new Thread(() -> {
//            try {
//                bootstrap.group(bossGroup, workerGroup);
//                bootstrap.channel(NioServerSocketChannel.class);
//                bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
//                    @Override
//                    protected void initChannel(SocketChannel ch) throws Exception {
//                        ChannelPipeline pipeline = ch.pipeline();
//                        pipeline.addLast(new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//                        pipeline.addLast(new LengthFieldPrepender(4));
//                        pipeline.addLast("encoder", new ObjectEncoder());
//                        pipeline.addLast("decoder", new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
//                        pipeline.addLast(new ChannelHandler());
//                    }
//                });
//                bootstrap.option(ChannelOption.SO_BACKLOG, 1024);
//                bootstrap.childOption(ChannelOption.SO_KEEPALIVE, true);
//                System.out.println("binding to port " + localAddress);
//                ChannelFuture channelFuture = bootstrap.bind(localAddress).sync();
//                channelFuture.channel().closeFuture().sync();
//            } catch (Exception e) {
//                e.printStackTrace();
//            } finally {
//                bossGroup.shutdownGracefully();
//                workerGroup.shutdownGracefully();
//            }
//        }).start();
//    }

//    class ChannelHandler extends ChannelInboundHandlerAdapter {
//
//        @Override
//        public void channelActive(ChannelHandlerContext ctx) throws Exception {
//            System.out.println("ChannelHandler channelActive...");
//        }
//
//        @Override
//        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//            System.out.println("ChannelHandler channelRead -> " + msg);
//            Vote vote = (Vote)msg;
//            if (vote.getServerId() == myId) {
//                if (vote.getElectionCode() == ElectionCode.Agree
//                        && election.getVote().getEpoch() == vote.getEpoch()) {
//                    recv(true);
//                    System.out.println("我的投票请求，别人通过了；本次通信结束。");
//                    return;
//                }
//                recv(false);
//                updateEpoch(vote);
//                System.out.println("别人的回应[更新epoch后]就此打住。");
//                return;
//            } else {
//                if (vote.getElectionCode() == ElectionCode.canvass
//                        && election.getVote().getEpoch() == vote.getEpoch()
//                        && pollMap.get(election.getVote().getEpoch()) == null) {
//                    System.out.println("对方请求投票 - 我未投票|选举朝代一致【给他】 vote.epoch = " + vote.getEpoch());
//                    pollMap.put(election.getVote().getEpoch(), true);
//                    vote.setElectionCode(ElectionCode.Agree);
//                    ctx.writeAndFlush(vote);
//                    return;
//                }
//                if (vote.getElectionCode() == ElectionCode.landlord) {
//                    self.setSevState(SevState.FOLLOWING);
//                    System.out.println("其他节点选举成功，直接跟随，数据原封不动丢回去！");
//                } else {
//                    vote.setElectionCode(ElectionCode.refuse);
//                    System.out.println("其他情况，全部拒绝 - " + StringUtils.getString(vote));
//                }
//                updateEpoch(vote);
//                vote.setSign("数据回写成功！");
//                ctx.writeAndFlush(vote);
//            }
//        }
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//            System.out.println("关闭连接：" + cause.getMessage());
//            ctx.close();
//        }
//    }


    /**********************************************************************************/

//    private LinkedBlockingQueue<Boolean> recvQueue = new LinkedBlockingQueue<>();
//    private LinkedBlockingQueue<Vote> sendQueue = new LinkedBlockingQueue<>();
//
//    public synchronized void recv(Boolean bool){
//        System.out.println("到底有没有收到，选票消息？");
//        recvQueue.add(bool);
//    }
//
//    public void clearRecvQueue() {
//        recvQueue.clear();
//    }
//
//    public Boolean takeRecv() throws InterruptedException {
//        return recvQueue.take();
//    }
//
//    public void sendVote(Vote vote) {
//        sendQueue.add(vote);
//    }

//    public class SendVoteWorker extends XiKVThread {
//
//        public SendVoteWorker() {
//            super("SendVoteWorker");
//        }
//
//        @Override
//        public void run() {
//            try {
//                while (!shutdown) {
//                    Vote vote = sendQueue.take();
//                    for (Integer sid : serverMap.keySet()) {
//                        if (sid == myId) {
//                            System.out.println("不给自己投票，计票时直接加就可以了, myId -> " + myId);
//                            continue;
//                        }
//                        try {
//                            InetSocketAddress addr = serverMap.get(sid).getElectionAddr();
//                            Socket socket = new Socket(addr.getHostName(), addr.getPort());
//                            OutputStream out = socket.getOutputStream();
//                            String voteStr = StringUtils.getString(vote);
//                            out.write(voteStr.getBytes());
//                            out.flush();
//                            socket.close();
//                            send(vote, addr);
//                        } catch (Exception e) {
//                            recvQueue.add(false);
//                            System.out.println("检查队列数据是否存在：" + recvQueue.size());
//                            System.out.println("拉选票失败，对方下线：" + e);
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public void send(Vote vote, InetSocketAddress addr){
//        EventLoopGroup group = new NioEventLoopGroup();
//        //ClientHandler clientHandler = new ClientHandler();
//        try {
//            Bootstrap bootstrap = new Bootstrap();
//            bootstrap.group(group)
//                    .channel(NioSocketChannel.class)
//                    .option(ChannelOption.TCP_NODELAY, true)
//                    .handler(new ChannelInitializer<SocketChannel>() {
//                        @Override
//                        public void initChannel(SocketChannel ch) throws Exception {
//                            ChannelPipeline pipeline = ch.pipeline();
//                            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
//                            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
//                            pipeline.addLast("encoder", new ObjectEncoder());
//                            pipeline.addLast("decoder", new ObjectDecoder(Integer.MAX_VALUE, ClassResolvers.cacheDisabled(null)));
//                            pipeline.addLast("handler", new ChannelHandler());
//                        }
//                    });
//
//            ChannelFuture future = bootstrap.connect(addr).sync();
//            future.channel().writeAndFlush(vote).sync();
//            future.channel().closeFuture().sync();
//        } catch (Exception e) {
//            e.printStackTrace();
//            recv(false);
//        } finally {
//            group.shutdownGracefully();
//        }
//    }

//    @io.netty.channel.ChannelHandler.Sharable
//    class ClientHandler extends ChannelInboundHandlerAdapter {
//
//        @Override
//        public void channelActive(ChannelHandlerContext ctx) throws Exception {
//            System.out.println("channelActive...");
//        }
//
//        @Override
//        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//            //Vote vote = (Vote) msg;
//            System.out.println("msg -> " + msg);
//        }
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//            cause.printStackTrace();
//            ctx.close();
//        }
//    }
}
