package com.xikv.server.cluster;

public interface ElectionCode {
    int CONTACTER = 1;
    int ACK_OK    = 2;
    int ACK_NO    = 3;
    int LEADER    = 9;
}
