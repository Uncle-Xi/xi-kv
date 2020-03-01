package com.xikv.server.network;

import com.xikv.common.thread.XiKVThread;
import com.xikv.server.Record;
import com.xikv.server.Request;

import java.io.IOException;

public abstract class ServerNet implements Record {

    Request request;

    public abstract void sendResponse(Record record) throws IOException;

    public Request getRequest() {
        return request;
    }

    public void setRequest(Request request) {
        this.request = request;
    }

}
