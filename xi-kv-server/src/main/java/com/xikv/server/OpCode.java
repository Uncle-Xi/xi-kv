package com.xikv.server;

public interface OpCode {

    int SET = 1;

    int DEL = 2; //

    int GET = 3;

    int SET_NX = 4;

    int INCR = 5;

    int DECR = 6;

    int EXISTS = 7; //

    int H_SET = 8;

    int H_GET = 9;

    int L_SET = 10;

    int L_GET = 11;

    int S_SET = 12;

    int S_GET = 13;

    int SIZE = 14;
}
