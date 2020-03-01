package com.xikv.server.memory;

/**
 * @description: SDS
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public class SDS {
    public int len;
    public int alloc;
    public int free;
    public char[] buf;
    public char flags;
}
