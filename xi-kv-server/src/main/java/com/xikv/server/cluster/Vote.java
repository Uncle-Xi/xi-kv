package com.xikv.server.cluster;

import io.netty.channel.ChannelHandlerContext;

import java.io.Serializable;

/**
 * @description: Vote
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Vote implements Serializable {

    private static final long serialVersionUID = 1L;

    private int serverId;
    private int epoch;
    private int electionCode;
    private Sync data;
    private transient ChannelHandlerContext ctx;

    public Vote() { }

    public Vote(int serverId, int epoch, int electionCode) {
        this(serverId, epoch, electionCode, null);
    }

    public Vote(int serverId, int epoch, int electionCode, Sync data) {
        this.serverId = serverId;
        this.epoch = epoch;
        this.electionCode = electionCode;
        this.data = data;
    }

    public int getServerId() {
        return serverId;
    }

    public void setServerId(int serverId) {
        this.serverId = serverId;
    }

    public int getEpoch() {
        return epoch;
    }

    public void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    public int getElectionCode() {
        return electionCode;
    }

    public void setElectionCode(int electionCode) {
        this.electionCode = electionCode;
    }

    public Sync getData() {
        return data;
    }

    public void setData(Sync data) {
        this.data = data;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public static class Sync implements Serializable{
        private String opCode;

        public String getOpCode() {
            return opCode;
        }

        public void setOpCode(String opCode) {
            this.opCode = opCode;
        }
    }

    public enum OpCode{
        SYNC,
    }
}
