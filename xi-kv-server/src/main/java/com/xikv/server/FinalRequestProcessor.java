package com.xikv.server;

import com.xikv.server.network.ServerNet;
import com.xikv.server.network.ServerNetty;

import java.io.IOException;

/**
 * @description: FinalRequestProcessor
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class FinalRequestProcessor implements RequestProcessor{

    QuorumPeer self;

    public FinalRequestProcessor(QuorumPeer self) {
        this.self = self;
    }

    @Override
    public void processRequest(Record record) throws RequestProcessorException {
        //System.out.println("FinalRequestProcessor processRequest...");
        ServerNet request = (ServerNet) record;
        Response response = self.getXiDB().options(request.getRequest());
        try {
            request.sendResponse(response);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
