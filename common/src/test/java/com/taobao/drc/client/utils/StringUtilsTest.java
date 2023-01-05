package com.taobao.drc.client.utils;

import org.junit.Test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class StringUtilsTest {

    @Test
    public void test() {
        String nullString = null;
        String emptyString = "";
        char sepator = ';';
        char otherSepator = ',';
        char emptySepator = ' ';
        String strToSpilt = "aa;bbb;ccc;d,sdf";
        String[] afterSplit = null;
        afterSplit = StringUtils.split(strToSpilt, sepator);
        assertTrue(afterSplit != null);
        assertTrue(afterSplit.length == 4);
        assertTrue(afterSplit[0].equals("aa"));
        assertTrue(afterSplit[1].equals("bbb"));
        assertTrue(afterSplit[2].equals("ccc"));
        assertTrue(afterSplit[3].equals("d,sdf"));
        afterSplit = StringUtils.split(strToSpilt, otherSepator);
        assertTrue(afterSplit.length == 2);
        assertTrue(afterSplit[0].equals("aa;bbb;ccc;d"));
        assertTrue(afterSplit[1].equals("sdf"));

        afterSplit = StringUtils.split(strToSpilt, emptySepator);
        assertTrue(afterSplit.length == 1);
        assertTrue(afterSplit[0].equals("aa;bbb;ccc;d,sdf"));
//		assertTrue(afterSplit[1].equals("sdf"));
//		System.out.println(afterSplit.toString());
        afterSplit = StringUtils.split(nullString, sepator);
        assertTrue(afterSplit == null);
        afterSplit = StringUtils.split(emptyString, sepator);
        assertTrue(afterSplit == null);
        afterSplit = StringUtils.split(strToSpilt, 'a');
        assertTrue(afterSplit.length == 1);
    }

    @Test
    public void  testStringMatch() {
        String str = "http://100.69.202.200:49616/c_da_tcbuyer-256-0";
        Pattern p = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+):(\\d+)");
        Matcher m = p.matcher(str);
        if(m.find()) {
            System.out.println("ip:port ~~" + m.group(0));
        }
    }
}
