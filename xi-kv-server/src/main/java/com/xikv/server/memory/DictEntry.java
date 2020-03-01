package com.xikv.server.memory;

/**
 * @description: DictEntry
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class DictEntry {

    public SDS key;

    static class Value {
        public XiKVObject val;
        public long u64;
        public long s64;
        double d;
    }

    public DictEntry next;

    static class XiKVObject{
        public int type;
        public int encoding;
        public int lru;
        int refcount;
        public SDS ptr;
    }
}
