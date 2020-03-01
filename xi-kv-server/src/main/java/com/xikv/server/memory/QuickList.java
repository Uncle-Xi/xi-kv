package com.xikv.server.memory;

/**
 * @description: QuickList
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class QuickList {

    public QuickListNode head;
    public QuickListNode tail;
    public long count;
    public long len;
    public int fill = 16;
    public int compress = 16;

    static class QuickListNode{
        public QuickListNode prev;
        public QuickListNode next;
        public ZipList zl;
        public int sz;
        public int count = 16;
        public int encoding = 2;
        public int container = 2;
        public int reCompress = 1;
        public int attempted_compress = 1;
        public int extra = 10;
    }
}
