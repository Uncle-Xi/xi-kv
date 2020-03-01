package com.xikv.server;

import com.xikv.common.thread.XiKVThread;
import com.xikv.common.util.StringUtils;
import com.xikv.server.network.ServerNet;
import com.xikv.server.network.ServerNetty;
import com.xikv.server.storage.StorageManager;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @description: SyncRequestProcessor
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SyncRequestProcessor extends XiKVThread implements RequestProcessor{

    private QuorumPeer self;
    private final LinkedBlockingQueue<Record> queuedRequests = new LinkedBlockingQueue<>();
    private final RequestProcessor nextProcessor;

    public SyncRequestProcessor(QuorumPeer self, RequestProcessor nextProcessor) {
        super("SyncRequestProcessor");
        this.self = self;
        this.nextProcessor = nextProcessor;
    }

    @Override
    public void run() {
        try {
            while (true){
                //System.out.println("[SyncRequestProcessor] 同步数据给 follower 节点，过半提交后，继续执行[TODO]...");
                ServerNet request = (ServerNet) queuedRequests.take();
                StorageManager storageManager = self.getStorageManager();
                storageManager.log(request.getRequest());
                if (nextProcessor != null) {
                    //System.out.println("SyncRequestProcessor run -> " + StringUtils.getString(request.getRequest()));
                    nextProcessor.processRequest(request);
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