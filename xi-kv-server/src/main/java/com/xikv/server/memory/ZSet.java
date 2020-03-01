package com.xikv.server.memory;

/**
 * @description: Zset
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ZSet {

    public Dict dict;
    public ZSkipList zsl;

    static class ZSkipList{
        public ZSkipListNode header, tail;
        public long length;
        public int level;
    }

    static class ZSkipListNode {
        public SDS ele;
        public double score;
        public ZSkipListLevel[] level;
        public ZSkipListNode backward;
    }

    static class ZSkipListLevel {
        public ZSkipListNode forward;
        public long span;
    }
}
