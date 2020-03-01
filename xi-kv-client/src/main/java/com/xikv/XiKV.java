package com.xikv;

import com.alibaba.fastjson.JSON;
import com.xikv.client.XiKVClient;
import com.xikv.common.util.StringUtils;
import com.xikv.server.OpCode;
import com.xikv.server.Request;
import com.xikv.server.Response;
import com.xikv.server.StatCode;
import com.xikv.server.memory.XiKVDatabase;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @description: XiKV
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiKV {

    private XiKVClient client;
    private String password;
    private String clientType = "API";

    public XiKV(String connectString, String password) {
        this.password = password;
        client = new XiKVClient(connectString, this);
        client.start();
    }

    private boolean check(String key, Object val) {
        if (val == null || key == null) {
            throw new RuntimeException("key or val is null");
        }
        return true;
    }

    private boolean checkKey(String key) {
        if (key == null) {
            throw new RuntimeException("key can not be null");
        }
        return true;
    }

    public boolean set(String key, String val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.SET);
        request.setKey(key);
        request.setValue(val);
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.error ? false : true;
    }

    public boolean del(String key) throws InterruptedException {
        if (key == null) {
            throw new RuntimeException("key is null");
        }
        Request request = new Request();
        request.setOpCode(OpCode.DEL);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.error ? false : true;
    }

    public String get(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.GET);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getContent();
    }

    public boolean setNx(String key, String val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.SET_NX);
        request.setKey(key);
        request.setValue(val);
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.empty ? true : false;
    }

    public int incr(String key, String val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.INCR);
        request.setKey(key);
        request.setValue(val);
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return Integer.valueOf(response.getContent());
    }

    public int decr(String key, String val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.DECR);
        request.setKey(key);
        request.setValue(val);
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return Integer.valueOf(response.getContent());
    }

    public boolean exists(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.EXISTS);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.exists ? true : false;
    }

    public boolean hSet(String key, Map val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.H_SET);
        request.setKey(key);
        request.setValue(StringUtils.getString(val));
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.HASH);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.error ? false : true;
    }

    public Map hGet(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.H_GET);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.HASH);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        if (response.getContent() != null) {
            return JSON.parseObject(response.getContent(), Map.class);
        }
        return null;
    }

    public boolean lSet(String key, List val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.L_SET);
        request.setKey(key);
        request.setValue(StringUtils.getString(val));
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.LIST);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.error ? false : true;
    }

    public List lGet(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.L_GET);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.LIST);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        if (response.getContent() != null) {
            return JSON.parseObject(response.getContent(), List.class);
        }
        return null;
    }

    public boolean sSet(String key, Set val, long timeout) throws InterruptedException {
        check(key, val);
        Request request = new Request();
        request.setOpCode(OpCode.S_SET);
        request.setKey(key);
        request.setValue(StringUtils.getString(val));
        request.setTimeout(timeout);
        request.setType(XiKVDatabase.Type.SET);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        return response.getCode() == StatCode.error ? false : true;
    }

    public Set sGet(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.S_GET);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.SET);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        if (response.getContent() != null) {
            return JSON.parseObject(response.getContent(), Set.class);
        }
        return null;
    }

    public int size(String key) throws InterruptedException {
        checkKey(key);
        Request request = new Request();
        request.setOpCode(OpCode.SIZE);
        request.setKey(key);
        request.setType(XiKVDatabase.Type.STRING);
        request.setPassword(password);
        request.setClientType(clientType);
        Response response = client.submitRequest(request);
        if (response.getContent() != null) {
            return Integer.valueOf(response.getContent());
        }
        return 0;
    }
}
