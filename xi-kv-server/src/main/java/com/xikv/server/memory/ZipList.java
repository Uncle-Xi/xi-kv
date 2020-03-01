package com.xikv.server.memory;

/**
 * @description: ZipList
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class ZipList {

    public int zlBytes;
    public ZlEntry zlTail;
    public int zlLen;
    public ZlEntry[] entry;
    public byte zlEnd;

    static class ZlEntry {
        public int prevRawLenSize;
        public int prevRawLen;
        public int lenSize;
        public int len;
        public int headerSize;
        public char encoding;
        public char[] p;
    }
}
