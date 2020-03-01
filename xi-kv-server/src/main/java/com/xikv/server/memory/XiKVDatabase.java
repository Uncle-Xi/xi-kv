package com.xikv.server.memory;

import com.xikv.common.thread.XiKVThread;
import com.xikv.common.util.StringUtils;
import com.xikv.server.OpCode;
import com.xikv.server.Request;
import com.xikv.server.Response;
import com.xikv.server.StatCode;
import com.xikv.server.config.Configuration;
import jdk.internal.dynalink.beans.BeansLinker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @description: XiKVDatabase
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class XiKVDatabase extends XiKVThread {

    private int DB_CAPACITY = 100;
    private String policy = "ALL_LRU";
    private int LRU_SAMPLES_SIZE = 5;
    private final Configuration config;
    private final Map<KEY, Object> DATA = new ConcurrentHashMap<>();
    private KEY[] KEY_TIMEOUT = new KEY[]{};

    public XiKVDatabase(Configuration config) {
        super("XiKVDatabase");
        this.config = config;
    }

    public int getDataSize() {
        return DATA.size();
    }

    public Response options(Request request) {
        Response response = new Response();
        switch (request.getOpCode()) {
            case OpCode.SET:
            case OpCode.H_SET:
            case OpCode.L_SET:
            case OpCode.S_SET:
                DATA.put(getKeyAndInsert(request), request.getValue());
                break;
            case OpCode.DEL:
                removeEle(new KEY(request.getKey(), 0, Type.STRING));
                break;
            case OpCode.GET:
            case OpCode.S_GET:
            case OpCode.L_GET:
            case OpCode.H_GET:
                get(request, response);
                break;
            case OpCode.SET_NX:
                nx(request, response);
                break;
            case OpCode.INCR:
                incr(request, response);
                break;
            case OpCode.DECR:
                decr(request, response);
                break;
            case OpCode.EXISTS:
                response.setCode(exists(getKey0(request)));
                break;
            case OpCode.SIZE:
                size(request, response);
                break;
            default:
                response.setCode(StatCode.error);
                response.setContent("空操作");
        }
        return response;
    }

    private void get(Request request, Response response){
        KEY key = getKey0(request);
        String val = null;
        Iterator<Map.Entry<KEY, Object>> iterator = DATA.entrySet().iterator();
        Map.Entry<KEY, Object> entry;
        while (iterator.hasNext()){
            entry = iterator.next();
            if (entry.getKey().equals(key)) {
                entry.getKey().lru = System.currentTimeMillis();
                val = (String) entry.getValue();
                break;
            }
        }
        response.setContent(val);
    }

    private void size(Request request, Response response){
        String key = request.getKey();
        int size = 0;
        for (KEY k : DATA.keySet()){
            if (k.key.startsWith(key)) {
                size ++;
            }
        }
        response.setContent(size + "");
    }

    private void nx(Request request, Response response) {
        synchronized (DATA) {
            KEY key = getKey0(request);
            if (exists(key) == StatCode.exists) {
                response.setCode(StatCode.exists);
            } else {
                response.setCode(StatCode.empty);
                insertAndSortKey(key);
                DATA.put(key, request.getValue());
            }
        }
    }


    private void incr(Request request, Response response) {
        synchronized (DATA) {
            response.setContent(cr(request, 1) + "");
        }
    }

    private void decr(Request request, Response response) {
        synchronized (DATA) {
            response.setContent(cr(request, -1) + "");
        }
    }

    private int cr(Request request, int di){
        KEY key = getKey0(request);
        int val;
        if (exists(key) == StatCode.exists) {
            val = (Integer) DATA.get(key);
        } else {
            insertAndSortKey(key);
            val = request.getValue() == null ? 0 : Integer.valueOf(request.getValue());
        }
        DATA.put(key, val = val + di);
        return val;
    }

    private int exists(KEY key) {
        return DATA.get(key) == null ? StatCode.empty : StatCode.exists;
    }

    private KEY getKey0(Request request) {
        KEY key = new KEY(request.getKey(), request.getTimeout() == null ? 0 : request.getTimeout(), request.getType());
        key.setOpCode(request.getOpCode());
        return key;
    }

    private KEY getKeyAndInsert(Request request) {
        KEY key = getKey0(request);
        if (request.getTimeout() != null && request.getTimeout() != 0) {
            insertAndSortKey(key);
        }
        return key;
    }

    public static void main(String[] args) {
        XiKVDatabase db = new XiKVDatabase(null);
        KEY key = new KEY("youXiu", 10, Type.HASH);
        KEY k = new KEY("youXiu", 0, Type.STRING);
        db.DATA.put(key, "KEY_youXiu");
        System.out.println(new KEY("youXiu", 0, Type.STRING).equals(k));
        //System.out.println(db.DATA.remove(new KEY("youXiu", 0, Type.STRING)));
        System.out.println(db.DATA.get(k));

        //Set<Integer> randoms = new HashSet<>(db.LRU_SAMPLES_SIZE);
        //for (int i = 0; i < db.LRU_SAMPLES_SIZE; i++) {
        //    int pre = randoms.size();
        //    randoms.add(new Random().nextInt(50));
        //    if (pre == randoms.size()) {
        //        i--;
        //    }
        //}
        //for (Integer i : randoms) {
        //    System.out.println(i);
        //}
    }

    public static class KEY {
        // LRU
        // LRU TODO
        public long lru;
        public long timeout;
        public Type type;
        public int opCode;
        public String key;

        public KEY(String key, long timeout, Type type) {
            this.key = key;
            this.lru = System.currentTimeMillis();
            this.timeout = timeout + lru;
            this.type = type;
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return this.key.equals(((KEY) obj).key);
        }

        public long getLru() {
            return lru;
        }

        public void setLru(long lru) {
            this.lru = lru;
        }

        public long getTimeout() {
            return timeout;
        }

        public void setTimeout(long timeout) {
            this.timeout = timeout;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public String getKey() {
            return key;
        }

        public void setKey(String key) {
            this.key = key;
        }

        public int getOpCode() {
            return opCode;
        }

        public void setOpCode(int opCode) {
            this.opCode = opCode;
        }
    }

    public void insertAndSortKey(KEY key) {
        if (key == null) {
            System.out.println("Ksy == null");
            return;
        }
        if (existsKey(key)) {
            //System.out.println("重複 Key update 即可");
            updateKey(key);
            return;
        }
        insertKey(key);
    }

    public void updateKey(KEY key){
        synchronized (this) {
            removeEle(key);
            insertKey(key);
        }
    }

    public void insertKey(KEY key){
        synchronized (this) {
            KEY[] keys = new KEY[KEY_TIMEOUT.length + 1];
            int idx = KEY_TIMEOUT.length;
            if (idx != 0) {
                for (int i = KEY_TIMEOUT.length - 1; i >= 0; i--) {
                    if (KEY_TIMEOUT[i] == null) {
                        continue;
                    }
                    if (KEY_TIMEOUT[i].timeout < key.timeout) {
                        keys[i + 1] = KEY_TIMEOUT[i];
                        idx = i;
                    }
                }
                for (int i = idx - 1; i >= 0; i--) {
                    keys[i] = KEY_TIMEOUT[i];
                }
            }
            keys[idx] = key;
            KEY_TIMEOUT = keys;
        }
    }

    public enum Type {
        STRING, HASH, SET, LIST;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (KEY_TIMEOUT.length == 0) {
                    Thread.sleep(10 * 1000);
                    // TODO 通知机制，condition AQS
                }
                long timeout;
                KEY key;
                synchronized (KEY_TIMEOUT) {
                    timeout = KEY_TIMEOUT[KEY_TIMEOUT.length - 1].timeout - System.currentTimeMillis();
                    key = KEY_TIMEOUT[KEY_TIMEOUT.length - 1];
                }
                if (timeout > 0) {
                    Thread.sleep(timeout);
                } else {
                    removeEle(key);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void removeEle(KEY key) {
        if (key == null) {
            System.out.println("[removeEle] [key == null]");
        }
        if (existsKey(key)) {
            synchronized (this) {
                KEY[] keys = new KEY[KEY_TIMEOUT.length - 1];
                for (int i = 0, j = 0; i < KEY_TIMEOUT.length; i++) {
                    if (KEY_TIMEOUT[i] == null) {
                        continue;
                    }
                    if (!KEY_TIMEOUT[i].equals(key)) {
                        keys[j++] = KEY_TIMEOUT[i];
                    }
                }
                KEY_TIMEOUT = keys;
            }
        }
        DATA.remove(key);
    }

    private boolean existsKey(KEY key) {
        for (KEY k : KEY_TIMEOUT) {
            if (key.equals(k)) {
                return true;
            }
        }
        return false;
    }

    public void memoryCheck() {
        if (DB_CAPACITY <= getDataSize()) {
            System.out.printf("[内存数据容量不足] [指定容量]=[%d], [实际容量]=[%d]; 开始[LRU]内存淘汰.\n", DB_CAPACITY, getDataSize());
            out4Memory();
            System.out.printf("[内存淘汰结束] [KEY_TIMEOUT.length]=[%d], [DATA.size]=[%d].\n", KEY_TIMEOUT.length, DATA.size());
        }
    }

    // 数据大小，数量超过预订值，随机取 5 个删除其中最久未使用的数据，直到满足条件为止
    private void out4Memory() {
        Set<KEY> setKey = DATA.keySet();
        int idx = 0, rid = 0;
        KEY[] rk = new KEY[LRU_SAMPLES_SIZE];
        Set<Integer> randoms = new HashSet<>(LRU_SAMPLES_SIZE);
        for (int i = 0; i < LRU_SAMPLES_SIZE; i++) {
            int pre = randoms.size();
            randoms.add(new Random().nextInt(DATA.size()));
            if (pre == randoms.size()) {
                i--;
            }
        }
        for (KEY k : setKey) {
            for (Integer i : randoms) {
                if (i == idx) {
                    rk[rid++] = k;
                }
            }
            idx++;
        }
        KEY earlyKay = null;
        long early = Long.MAX_VALUE;
        for (KEY k : rk) {
            if (k.lru < early) {
                early = k.lru;
                earlyKay = k;
            }
        }
        if (earlyKay == null) {
            System.out.println("earlyKay == null");
            return;
        }
        removeEle(earlyKay);
    }

    public Map<KEY, Object> getDATA() {
        return DATA;
    }

    public KEY[] getKEY_TIMEOUT() {
        return KEY_TIMEOUT;
    }
}
