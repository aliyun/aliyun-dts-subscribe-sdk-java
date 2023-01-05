package com.taobao.drc.client;

import org.junit.Test;
import static org.junit.Assert.*;

public class BinlogPosTest {

    @Test
    public void test() {
        String filename = "mysql-bin.001024";
        String pos = "1024";
        BinlogPos binlogPos = new BinlogPos(filename, pos);
        System.out.println("binlog pos is : " + binlogPos.toString());
        assertTrue(binlogPos.toString().equals(pos + "@" + filename));
    }
}
