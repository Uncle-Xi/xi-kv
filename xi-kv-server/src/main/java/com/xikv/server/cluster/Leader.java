package com.xikv.server.cluster;

import com.xikv.common.util.StringUtils;
import com.xikv.server.*;
import com.xikv.server.config.Configuration;
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: Leader
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Leader extends Elector{

    volatile boolean statChange = false;
    private LinkedBlockingQueue<Vote> recvQueue = new LinkedBlockingQueue<>();

    public Leader(QuorumPeer self) throws IOException {
        super(self, "Leader...");
        this.self = self;
        this.serverId = this.self.configuration.getServerId();
        this.serverMap = this.self.configuration.getServers();
        this.server = this.serverMap.get(serverId);
        this.port = server.getPort();
        this.vote = new Vote(serverId, getEpochByFile(ELECTION_EPOCH_FILENAME), ElectionCode.LEADER);
        this.self.getChannelHandler().setElector(this);
    }

    public void lead(){
        System.out.println(
                "1 > 启动数据处理责任链； \n" +
                "2 > 监听数据同步请求【网络问题带来的票据请求】，将数据返回给 follower； \n" +
                "3 > 定期发送心跳保证自己 leader 状态...\n");
        setupRequestProcessors();
        while (!statChange) {
            try {
                for (Integer sid : serverMap.keySet()) {
                    if (sid == serverId) {
                        //System.out.println("[Leader] 不给自己发心跳, serverId -> " + serverId);
                        continue;
                    }
                    try {
                        InetSocketAddress addr = serverMap.get(sid).getElectionAddr();
                        //send(addr, new LSCHandler(), vote);
                        SendVote.send(addr, vote);
                    } catch (Exception e) {
                        System.out.println("[Leader] [SendVote][Exception] [对方下线]:" + e);
                        //e.printStackTrace();
                    }
                }
                System.out.println("[Leader] [频率:100s] [PING] -> [Follower].");
                Thread.sleep(100 * 1000);
            } catch (Exception e){
                e.printStackTrace();
                System.out.println("[Leader] Leader Exception: ");
            }
        }
    }

    @Override
    public void run() { }

    @Override
    public void resolve(Vote v) throws IOException {
        try {
            System.out.println("[Leader] [允许状态为] [CONTACTER] [收到状态][" + vote.getElectionCode() + "]");
            v.setEpoch(electionEpoch);
            v.setElectionCode(ElectionCode.LEADER);
            if (ElectionCode.LEADER == v.getElectionCode()) {
                System.err.println("[Leader] [对方状态（LEADER）非法].");
            }
            if (ElectionCode.CONTACTER == v.getElectionCode()) {
                System.out.println("[Leader] 遇到从节点被唤醒, 修它 > " + StringUtils.getString(v));
            }
            if (v.getData() != null) {
                System.out.println("[Leader] 将本地快照发给对方，注意，全量，增量，快照标记范围...");// TODO
            }
            v.getCtx().write(v);
            v.getCtx().flush();
            v.getCtx().close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    public void setupRequestProcessors() {
        RequestProcessor finalProcessor = new FinalRequestProcessor(self);
        RequestProcessor syncProcessor = new SyncRequestProcessor(self, finalProcessor);
        firstProcessor = new PrepRequestProcessor(self, syncProcessor);
        ((SyncRequestProcessor) syncProcessor).start();
        ((PrepRequestProcessor) firstProcessor).start();
        self.setFirstProcessor(firstProcessor);
    }

    @Override
    protected void setSevState(SevState state) {
        System.out.println("[Leader] 修改自己的角色为 [" + state + "], 结束线程循环...");
        self.setSevState(state);
        shutdown();
    }

    @Override
    protected synchronized void shutdown(){
        statChange = true;
        self.getChannelHandler().setElector(null);
    }
}
