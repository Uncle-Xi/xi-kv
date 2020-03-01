package com.xikv.server;

import com.xikv.common.thread.XiKVThread;
import com.xikv.server.network.ServerNet;
import com.xikv.server.network.ServerNetty;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: SyncRequestProcessor
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class PrepRequestProcessor extends XiKVThread implements RequestProcessor{

    private QuorumPeer self;
    private String password;
    private final LinkedBlockingQueue<Record> queuedRequests = new LinkedBlockingQueue<>();
    private final RequestProcessor nextProcessor;

    public PrepRequestProcessor(QuorumPeer self, RequestProcessor nextProcessor) {
        super("PrepRequestProcessor");
        this.self = self;
        this.password = self.configuration.getRequirePass();
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while (true){
                ServerNet request = (ServerNet) queuedRequests.take();
                Request req = request.getRequest();
                if ("CLI".equalsIgnoreCase(req.getClientType())
                        || password == null
                        || (password != null && password.equals(req.getPassword()))) {
                    if (req.getOpCode() == OpCode.SET
                            || req.getOpCode() == OpCode.H_SET
                            || req.getOpCode() == OpCode.L_SET
                            || req.getOpCode() == OpCode.S_SET) {
                        self.getXiDB().memoryCheck();
                    }
                    if (nextProcessor != null) {
                        //System.out.println("PrepRequestProcessor run -> " + request);
                        nextProcessor.processRequest(request);
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void processRequest(Record record) throws RequestProcessorException {
        queuedRequests.add(record);
    }
}