package com.xikv.server.cli.controller;

import com.xikv.server.*;
import com.xikv.server.memory.XiKVDatabase;
import com.xikv.server.network.NettyServerFactory;
import com.xikv.server.network.ServerNet;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URL;

/**
 * @description: CommandController
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class CommandController extends SimpleChannelInboundHandler<FullHttpRequest> {

    public static final String CHARSET_UTF8 = "UTF-8";
    public static final String HTTP_HEAD = "HTTP/1.1 200 OK";
    public static final String HTTP_CONTENT_KEY = "Content-Type";
    public static final String DEFAULT_HTTP_CONTENT_VAL = "text/html;charset=utf-8";

    QuorumPeer peer;
    NettyServerFactory factory;
    ChannelHandlerContext ctx;
    FullHttpRequest fullHttpRequest;

    public CommandController(QuorumPeer peer, NettyServerFactory factory) {
        this.peer = peer;
        this.factory = factory;
    }

    class CommandServer extends ServerNet {

        QuorumPeer peer;
        NettyServerFactory factory;

        public CommandServer(QuorumPeer peer, NettyServerFactory factory) {
            this.peer = peer;
            this.factory = factory;
        }

        @Override
        public void sendResponse(Record record) throws IOException {
            Response response = (Response) record;
            if (StatCode.error == response.getCode()) {
                response(ctx, fullHttpRequest, "FAIL");
            } else {
                if (OpCode.SET == getRequest().getOpCode()
                        || OpCode.DEL == getRequest().getOpCode()) {
                    response(ctx, fullHttpRequest, "OK");
                } else if (OpCode.EXISTS == getRequest().getOpCode()) {
                    response(ctx, fullHttpRequest, response.getCode() == StatCode.exists ? "true" : "false");
                } else {
                    response(ctx, fullHttpRequest, response.getContent() == null ? "" : response.getContent());
                }
            }
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        this.ctx = ctx;
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
        this.fullHttpRequest = request;
        String uri = request.getUri();
        String[] args = uri.split("\\?");
        // /set?key=value
        // get,set,del,exists,size
        System.out.println("[命令行請求參數]=[" + uri + "].");
        String opCode = args[0].replaceAll("/", "");
        String[] kv = args[1].split("=");
        String key = kv[0];
        String val = kv.length > 1 ? kv[1] : null;
        CommandServer commandServer = new CommandServer(peer, factory);
        Request req = new Request();
        req.setOpCode(opCode.equals("set") ? OpCode.SET
                : opCode.equals("get") ? OpCode.GET
                : opCode.equals("del") ? OpCode.DEL
                : opCode.equals("exists") ? OpCode.EXISTS : OpCode.SIZE);
        req.setKey(key);
        req.setValue(val);
        req.setTimeout((long) Integer.MAX_VALUE);
        req.setType(XiKVDatabase.Type.STRING);
        req.setClientType("CLI");
        commandServer.setRequest(req);
        peer.processConnectRequest(commandServer);
    }

    public void response(ChannelHandlerContext ctx, FullHttpRequest req, String resp) throws IOException {
        try {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1,
                    HttpResponseStatus.OK,
                    Unpooled.wrappedBuffer(resp.getBytes(CHARSET_UTF8)));
            response.headers().set(HTTP_CONTENT_KEY, DEFAULT_HTTP_CONTENT_VAL);
            response.headers().set("vary", "Accept-Encoding");
            boolean keepAlive = HttpHeaders.isKeepAlive(req);
            if (keepAlive) {
                response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, resp.length());
                response.headers().set(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
            }
            ChannelFuture future = ctx.writeAndFlush(LastHttpContent.EMPTY_LAST_CONTENT);
            if (!keepAlive) {
                future.addListener(ChannelFutureListener.CLOSE);
            }
            ctx.write(response);
        } finally {
            ctx.flush();
            ctx.close();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        Channel client = ctx.channel();
        cause.printStackTrace();
        ctx.close();
    }
}
