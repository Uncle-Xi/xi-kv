package com.xikv.server;

import com.xikv.server.memory.XiKVDatabase;

/**
 * @description: Request
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Request implements Record {

    private int opCode;
    private String key;
    private String value;
    private Long timeout;
    private XiKVDatabase.Type type;
    private String password;
    private String clientType;

    public Request(){ }


    public Request(int opCode, String key, String value, Long timeout, XiKVDatabase.Type type) {
        this.opCode = opCode;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
        this.type = type;
    }

    public Request(int opCode, String key, String value, Long timeout, XiKVDatabase.Type type, String clientType) {
        this.opCode = opCode;
        this.key = key;
        this.value = value;
        this.timeout = timeout;
        this.type = type;
        this.clientType = clientType;
    }

    public int getOpCode() {
        return opCode;
    }

    public void setOpCode(int opCode) {
        this.opCode = opCode;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public XiKVDatabase.Type getType() {
        return type;
    }

    public void setType(XiKVDatabase.Type type) {
        this.type = type;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getClientType() {
        return clientType;
    }

    public void setClientType(String clientType) {
        this.clientType = clientType;
    }
}
