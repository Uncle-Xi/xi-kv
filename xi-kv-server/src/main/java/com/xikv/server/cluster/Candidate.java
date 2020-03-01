package com.xikv.server.cluster;

import com.xikv.common.util.StringUtils;
import com.xikv.server.QuorumPeer;
import com.xikv.server.SevState;
import com.xikv.server.config.Configuration;
import io.netty.channel.ChannelHandlerContext;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @description: RaftElection
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Candidate extends Elector {

    /**
     * We also have a client that can send a value to the server.
     * Coming to agreement, or consensus, on that value is easy with one node.
     * But how do we come to consensus if we have multiple nodes?
     * That's the problem of distributed consensus.
     * Raft is a protocol for implementing distributed consensus.
     * Let's look at a high level overview of how it works.
     * A node can be in 1 of 3 states:
     * The Follower state,the Candidate state,or the Leader state.
     * All our nodes start in the follower state.
     * If followers don't hear from a leader then they can become a candidate.
     * The candidate then requests votes from other nodes.
     * Nodes will reply with their vote.
     * The candidate becomes the leader if it gets votes from a majority of nodes.
     * This process is called Leader Election.
     * All changes to the system now go through the leader.
     * Each change is added as an entry in the node's log.
     * This log entry is currently uncommitted so it won't update the node's value.
     * To commit the entry the node first replicates it to the follower nodes...
     * then the leader waits until a majority of nodes have written the entry.
     * The entry is now committed on the leader node and the node state is "5".
     * The leader then notifies the followers that the entry is committed.
     * The cluster has now come to consensus about the system state.
     * This process is called Log Replication.
     * <p>
     * In Raft there are two timeout settings which control elections.
     * First is the election timeout.
     * The election timeout is the amount of time a follower waits until becoming a candidate.
     * The election timeout is randomized to be between 150ms and 300ms.
     * After the election timeout the follower becomes a candidate and starts a new election term...
     * ...votes for itself...
     * ...and sends out Request Vote messages to other nodes.
     * If the receiving node hasn't voted yet in this term then it votes for the candidate...
     * ...and the node resets its election timeout.
     * Once a candidate has a majority of votes it becomes leader.
     * The leader begins sending out Append Entries messages to its followers.
     * These messages are sent in intervals specified by the heartbeat timeout.
     * Followers then respond to each Append Entries message.
     * This election term will continue until a follower stops receiving heartbeats and becomes a candidate.
     * <p>
     * Let's stop the leader and watch a re-election happen.
     * Node B is now leader of term 2.
     * Requiring a majority of votes guarantees that only one leader can be elected per term.
     * If two nodes become candidates at the same time then a split vote can occur.
     * Let's take a look at a split vote example...
     * <p>
     * Two nodes both start an election for the same term...
     * ...and each reaches a single follower node before the other.
     * Now each candidate has 2 votes and can receive no more for this term.
     * Node D received a majority of votes in term 5 so it becomes leader.
     * <p>
     * Once we have a leader elected we need to replicate all changes to our system to all nodes.
     * This is done by using the same Append Entries message that was used for heartbeats.
     * Let's walk through the process.
     * First a client sends a change to the leader.
     */
    volatile boolean statChange = false;
    private LinkedBlockingQueue<Boolean> recvQueue = new LinkedBlockingQueue<>();
    private Map<Integer, Boolean> pollMap = new HashMap<>();

    public Candidate(QuorumPeer self) throws IOException {
        super(self, "Candidate");
        self.getChannelHandler().setElector(this);
    }

    @Override
    public void resolve(Vote vote) {
        try {
            if (ElectionCode.LEADER == vote.getElectionCode()) {
                System.out.println("[Candidate] [对方如果是leader] [设置自己状态为 follower], [更新 epoch 为对方的], 结束选举状态...");
                setSevState(SevState.FOLLOWING);
                updateEpoch(vote);
                vote.setElectionCode(ElectionCode.ACK_OK);
            } else if (ElectionCode.CONTACTER == vote.getElectionCode()) {
                //System.out.println("[Candidate] [对方也是选举人] [自己未投票就投票给他] [对方 epoch 高于自己，自己结束选举]...");
                if (vote.getEpoch() >= this.vote.getEpoch()) {
                    System.out.println("[Candidate] [对方Epoch大于等于自己的Epoch] [对方Epoch][ "
                            + vote.getEpoch() + "], [我的Epoch][" + this.vote.getEpoch() + "].");
                    setSevState(SevState.FOLLOWING);
                    updateEpoch(vote);
                    vote.setElectionCode(ElectionCode.ACK_OK);
                } else {
                    //System.out.println("[Candidate] 其他情况，如果我没有投票，响应 OK，否则响应 NO.");
                    vote.setElectionCode(ElectionCode.ACK_OK);
                    Boolean polled = pollMap.get(vote.getEpoch());
                    if (polled != null && polled) {
                        System.out.println("[Candidate] 我已经投票给了其他人, 不再投票给你...");
                        vote.setElectionCode(ElectionCode.ACK_NO);
                    } else {
                        pollMap.put(vote.getEpoch(), true);
                        System.out.println("[Candidate] 把票投给他...");
                    }
                }
            } else {
                System.out.println("[Candidate] [允许状态为] [LEADER|CONTACTER] [收到状态][" + vote.getElectionCode() + "]");
            }
            vote.getCtx().write(vote);
            vote.getCtx().flush();
            vote.getCtx().close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void leaderSelector() throws Exception {
        int clusterNum = serverMap.size();
        while (isLooking() && !statChange) {
            try {
                this.vote.setElectionCode(ElectionCode.CONTACTER);
                this.vote.setEpoch(incrEpoch(ELECTION_EPOCH_FILENAME));
                System.out.printf("[Candidate] [开始投票] [clusterNum] = [%d], [myVote] = [%s]\n",
                        clusterNum, StringUtils.getString(this.vote));
                this.sendVote(this.vote);
                int validCount = 1;
                for (int i = 0; i < clusterNum - 1; i++) {
                    if (this.takeRecv()) {
                        ++validCount;
                    }
                }
                if (!isLooking()) {
                    System.out.println("[Candidate] 已经选举出来了 leader, 结束...");
                    setSevState(SevState.FOLLOWING);
                    return;
                }
                if (overHalf(clusterNum, validCount)) {
                    System.out.printf("[Candidate] 票据过半，选举获胜, clusterNum = [%d]; validCount = [%d]\n", clusterNum, validCount);
                    this.vote.setElectionCode(ElectionCode.LEADER);
                    //this.sendVote(vote);
                    setSevState(SevState.LEADING);
                    return;
                }
                long sleepTime = randomSleepTime();
                Thread.sleep(sleepTime);
                System.out.println("[Candidate] [随机睡眠] [" + sleepTime + "] [开始下一轮选举]...");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isLooking() {
        return self.getSevState().equals(SevState.LOOKING);
    }

    private void sendVote(Vote vote) {
        System.out.println("[Candidate] 发送选票前，历史遗留数据 [" + recvQueue.size() + "].");
        //recvQueue.clear();
        for (Integer sid : serverMap.keySet()) {
            if (sid == serverId) {
                continue;
            }
            try {
                InetSocketAddress addr = serverMap.get(sid).getElectionAddr();
                //send(addr, new CSCHandler(), vote);
                inspect((Vote) SendVote.send(addr, vote));
            } catch (Throwable e) {
                //e.printStackTrace();
                recvQueue.offer(new Boolean(false));
                System.out.println("[Candidate] [对方不在线] [记一次失败][" + false + "][sid][" + sid + "]");
            }
        }
        //System.out.println("[Candidate] [选票发送完毕]");
    }

    private void inspect(Vote vote) throws IOException {
        if (vote == null) {
            System.out.println("[Candidate][inspect] [vote == null].");
            recvQueue.offer(new Boolean(false));
            return;
        }
        if (ElectionCode.LEADER == vote.getElectionCode()) {
            System.out.println("[Candidate][inspect][对方是leader][设置自己状态为Follower][更新Epoch为对方的][结束选举状态].");
            recvQueue.offer(new Boolean(false));
            setSevState(SevState.FOLLOWING);
            updateEpoch(vote);
        } else if (ElectionCode.ACK_OK == vote.getElectionCode()) {
            System.out.println("[Candidate][inspect][对方同意投票][计票+1...]");
            recvQueue.offer(new Boolean(true));
        } else if (ElectionCode.ACK_NO == vote.getElectionCode()) {
            System.out.println("[Candidate][inspect][对方拒绝投票][--]");
            recvQueue.offer(new Boolean(false));
        } else {
            throw new RuntimeException("非法状态 > " + StringUtils.getString(vote));
        }
    }

//    class CSCHandler extends ChannelHandler {
//        @Override
//        public void channelActive(ChannelHandlerContext ctx) throws Exception {
//            System.out.println("CSCHandler channelActive...");
//        }
//
//        @Override
//        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
//            System.out.println("CSCHandler channelRead [只会受到自己发出票的响应]:[" + StringUtils.getString(msg) + "].");
//            Vote vote = (Vote) msg;
//            if (ElectionCode.LEADER == vote.getElectionCode()) {
//                System.out.println("[对方是leader] [设置自己状态为 follower], [更新 epoch 为对方的], 结束选举状态...");
//                recvQueue.offer(new Boolean(false));
//                setSevState(SevState.FOLLOWING);
//                updateEpoch(vote);
//            } else if (ElectionCode.ACK_OK == vote.getElectionCode()) {
//                System.out.println("[对方同意投票][什么都不做...]");
//                recvQueue.offer(new Boolean(true));
//
//            } else if (ElectionCode.ACK_NO == vote.getElectionCode()) {
//                System.out.println("[对方拒绝投票][什么都不做...]");
//                recvQueue.offer(new Boolean(false));
//            } else {
//                throw new RuntimeException("非法状态 > " + StringUtils.getString(msg));
//            }
//            System.out.println("[Candidata] 发送反馈...");
//            ctx.writeAndFlush(vote);
//            System.out.println("你在这里等结果吗？");
//        }
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//            System.out.println("关闭连接：" + cause.getMessage());
//            ctx.close();
//        }
//
//        @Override
//        public Vote receiver(long timeout, TimeUnit timeUnit) throws InterruptedException {
//            return null;
//        }
//    }

    public Boolean takeRecv() throws InterruptedException {
        return recvQueue.poll(5, TimeUnit.SECONDS);
    }

    @Override
    protected void setSevState(SevState state) {
        System.out.println("[Candidate] [修改自己的角色] -> [" + state + "] [结束线程循环]...");
        self.setSevState(state);
        shutdown();
    }

    @Override
    protected synchronized void shutdown() {
        statChange = true;
        self.getChannelHandler().setElector(null);
    }
}
