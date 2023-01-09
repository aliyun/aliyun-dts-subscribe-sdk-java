package com.taobao.drc.client;

public class BinlogPos {

    private String fileNum;

    private String positionNum;

    public final static String AT = "@";

    public BinlogPos(final String file, final String pos) {
        fileNum = file;
        positionNum = pos;
    }

    @Override
    public String toString() {
        return positionNum + AT + fileNum;
    }
}
