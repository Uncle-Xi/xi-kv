package com.xikv.server.memory;

/**
 * @description: Dict
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class Dict {

    public DictType type;
    public Object priData;
    public DictHt[] ht = new DictHt[2];
    public long reHashIdx;
    public long iterators;

    static class DictType{

    }

    static class DictHt {
        public DictEntry table;
        public long size;
        public long sizeMask;
        public long used;
    }
}
