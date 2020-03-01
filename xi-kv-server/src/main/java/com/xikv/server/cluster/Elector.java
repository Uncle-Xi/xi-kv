package com.xikv.server.cluster;

import com.xikv.common.thread.XiKVThread;
import com.xikv.server.QuorumPeer;
import com.xikv.server.RequestProcessor;
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
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public abstract class Elector extends XiKVThread {

    public int electionEpoch = -1;
    public static String ELECTION_EPOCH_FILENAME = "electionEpoch";

    protected Configuration config;
    protected QuorumPeer self;
    protected int port;
    protected int serverId;
    protected Configuration.Server server;
    protected Map<Integer, Configuration.Server> serverMap;
    protected Vote vote;
    protected RequestProcessor firstProcessor;
    //protected Map<InetSocketAddress, Send> sendMap = new HashMap<>();

    public Elector(QuorumPeer self, String thredName) throws IOException {
        super(thredName);
        this.self = self;
        this.config = this.self.configuration;
        this.serverId = this.config.getServerId();
        this.serverMap = this.config.getServers();
        this.server = this.serverMap.get(serverId);
        this.port = server.getPort();
        this.electionEpoch = getEpochByFile(ELECTION_EPOCH_FILENAME);
        this.vote = new Vote(serverId, electionEpoch, ElectionCode.LEADER);
    }

    public void send(InetSocketAddress addr, ChannelHandler handler, Object data) {
        EventLoopGroup group = new NioEventLoopGroup();
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
                            pipeline.addLast("handler", handler);
                        }
                    });
            ChannelFuture future = bootstrap.connect(addr).sync();
            future.channel().writeAndFlush(data).sync();
            future.channel().closeFuture().sync();
        } catch (Throwable e) {
            e.printStackTrace();
            throw new RuntimeException("[连接失败][拒绝连接]");
        } finally {
            group.shutdownGracefully();
        }
    }

    //protected class Send{ }

    protected void updateEpoch(Vote vote) throws IOException {
        if (this.vote.getEpoch() < vote.getEpoch()) {
            this.vote.setEpoch(vote.getEpoch());
            this.electionEpoch = vote.getEpoch();
            setEpoch(Candidate.ELECTION_EPOCH_FILENAME, this.electionEpoch);
            System.out.println("[更新Epoch] [对方Epoch][ " + vote.getEpoch() + "], [我的Epoch][" + this.vote.getEpoch() + "].");
        }
    }

    public int setEpoch(String name, int epoch) throws IOException {
        synchronized (this) {
            File file = new File(config.getStorage(), name);
            if (!file.exists()) {
                file.createNewFile();
            }
            //System.out.println(file.getAbsolutePath());
            BufferedWriter br = new BufferedWriter(new FileWriter(file));
            try {
                br.write(String.valueOf(epoch));
                return epoch;
            } catch (NumberFormatException e) {
                throw new IOException("Found " + electionEpoch + " in " + file);
            } finally {
                br.close();
            }
        }
    }

    protected int incrEpoch(String name) throws IOException {
        File file = new File(config.getStorage(), name);
        if (!file.exists()) {
            file.createNewFile();
        }
        //System.out.println(file.getAbsolutePath());
        BufferedWriter br = new BufferedWriter(new FileWriter(file));
        try {
            br.write(String.valueOf(++electionEpoch));
            return electionEpoch;
        } catch (NumberFormatException e) {
            throw new IOException("Found " + electionEpoch + " in " + file);
        } finally {
            br.close();
        }
    }

    protected int getEpochByFile(String name) throws IOException {
        File file = new File(config.getStorage(), name);
        if (!file.exists()) {
            file.createNewFile();
            //try {
            //    System.out.println("file 路径 > " + file.getAbsolutePath());
            //    System.out.println("file name > " + name);
            //    System.out.println("file config.getStorage() > " + config.getStorage());
            //    Thread.sleep(5 * 1000);
            //} catch (Exception e) {
            //    e.printStackTrace();
            //}
        }
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line = "1";
        try {
            line = br.readLine();
            line = line == null ? "0" : line.trim();
            line = line.equals("") ? "0" : line;
            return electionEpoch = Integer.valueOf(line);
        } catch (NumberFormatException e) {
            throw new IOException("Found " + line + " in " + file);
        } finally {
            br.close();
        }
    }

    public boolean overHalf(int total, int have) {
        return have > total / 2;
    }

    public int randomSleepTime() {
        int max = 300, min = 150;
        return (int) (Math.random() * (max - min) + min);
    }

    protected Timer timer;

    protected void startTimerTask() {
        long timeout = 15 * 1000;
        timer = new Timer();
        timer.schedule(new ListenerTimerTask(), timeout);
        //System.out.println("启动一个定时器，到时间没有被 ping 就进入 Candidate...");
    }

    protected void stopTimer() {
        if (timer != null) {
            timer.cancel();
        }
        //System.out.println("收到了 ping ，清理掉原来的定时器...");
    }

    class ListenerTimerTask extends TimerTask {
        @Override
        public void run() {
            System.out.println("[ListenerTimerTask] [没有收到 ping] [准备进入 Candidate 角色]...");
            setSevState(SevState.LOOKING);
        }
    }

    protected abstract void setSevState(SevState state);

    protected abstract void shutdown();

    public abstract void resolve(Vote vote) throws IOException;
}
