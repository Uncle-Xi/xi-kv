package com.xikv.server.cluster;

import com.xikv.common.util.StringUtils;
import com.xikv.server.*;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: Follower
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Follower extends Elector {

    volatile boolean statChange = false;
    volatile boolean startSync = false;
    private Configuration.Server leader;
    private ChannelHandler channelHandler;
    private LinkedBlockingQueue<Vote> recvQueue = new LinkedBlockingQueue<>();
    private Map<Integer, Boolean> pollMap = new HashMap<>();

    public Follower(QuorumPeer self) throws IOException {
        super(self, "Follower");
        this.channelHandler = self.getChannelHandler();
        this.self.getChannelHandler().setElector(null);
        this.startTimerTask();
    }

    @Override
    public void resolve(Vote vote) throws IOException {
        //System.out.println("[Follower] [resolve] 允许，leader 的心跳票, 允许，Candidate 的选举票");
        if (vote == null) {
            return;
        }
        if (ElectionCode.LEADER == vote.getElectionCode()) {
            //System.out.println("[Follower] [Recv][Leader心跳票]");
            stopAndStartNew();
            vote.setElectionCode(ElectionCode.ACK_OK);
            leader = serverMap.get(vote.getServerId());
        } else if (ElectionCode.CONTACTER == vote.getElectionCode()){
            //System.out.println("[Follower] 如果未投票给其他人，就投票给他，否则？； 同时更新自己 epoch 到最新");
            vote.setElectionCode(ElectionCode.ACK_OK);
            Boolean polled = pollMap.get(vote.getEpoch());
            if (polled != null && polled) {
                System.out.println("[Follower] 我已经投票给了其他人, 不再投票给你...");
                vote.setElectionCode(ElectionCode.ACK_NO);
            } else {
                pollMap.put(vote.getEpoch(), true);
                System.out.println("[Follower] 把票投给他...");
            }
        } else {
            System.out.println("[Follower] [允许状态为][LEADER|CONTACTER] [收到状态][" + vote.getElectionCode() + "]");
        }
        vote.getCtx().write(vote);
        vote.getCtx().flush();
        vote.getCtx().close();
        updateEpoch(vote);
        startup();
    }

    public void follow() {
        //System.out.println("[Follower] 启动定时器，收到leader心跳，开始同步数据, 超时器到期后，进入 looking 模式...");
        while (!statChange) {
            try {
                this.resolve(channelHandler.receiver(5, TimeUnit.SECONDS));
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("[Follower] follow Exception: ");
            }
        }
    }

    protected void stopAndStartNew(){
        System.out.println("[Follower] [Recv][PING] [取消原来的定时器][新建定时器].");
        stopTimer();
        startTimerTask();
    }

    protected void startup(){
        if (!startSync && leader != null) {
            System.out.println("[Follower] [startup] [只需执行一次]...");
            startSync = true;
            syncData();
            setupRequestProcessors();
        }
    }

    public void setupRequestProcessors() {
        System.out.println("[Follower] 启动数据处理责任链[TODO]..."); // TODO
        RequestProcessor finalProcessor = new FinalRequestProcessor(self);
        RequestProcessor syncProcessor = new SyncRequestProcessor(self, finalProcessor);
        firstProcessor = new PrepRequestProcessor(self, syncProcessor);
        ((SyncRequestProcessor) syncProcessor).start();
        ((PrepRequestProcessor) firstProcessor).start();
        self.setFirstProcessor(firstProcessor);
    }

    protected Vote getVote() throws InterruptedException {
        return recvQueue.poll(15, TimeUnit.SECONDS);
    }

    protected void syncData() {
        System.out.println("[Follower] 启动数据同步[TODO]..."); // TODO
        Vote v = new Vote();
        v.setServerId(serverId);
        //send(leader.getElectionAddr(), new FSCHandler(), v)
        //SendVote.send(leader.getElectionAddr(), v);
    }

    class FSCHandler extends ChannelHandler {
        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            //System.out.println("ChannelHandler channelActive...");
        }
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            System.out.println("FSCHandler channelRead " + StringUtils.getString(msg));
            System.out.println("将 leader 发过来的快照，存入本地[TODO]..."); // TODO
        }
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("关闭连接：" + cause.getMessage());
            ctx.close();
        }
        @Override
        public Vote receiver(long timeout, TimeUnit timeUnit) throws InterruptedException {
            return null;
        }
    }

    @Override
    protected void setSevState(SevState state) {
        System.out.println("[Follower] [修改自己的角色] -> [" + state + "] [结束线程循环]...");
        self.setSevState(state);
        shutdown();
    }

    @Override
    protected synchronized void shutdown() {
        statChange = true;
        System.out.println("[Follower] [TODO] [清理任务处理责任链]");
        self.getChannelHandler().setElector(null);
    }
}
