package com.taobao.drc.client.utils;

import java.util.ArrayList;
import java.util.List;

public class StringUtils
{
	/**
	 * Split a string by one separator character. The performance
	 * is better than Java String split.   
	 * 
	 * Thanks to guzhen's share.
	 * 
	 * @param str is the string need be split.
	 * @param separatorChar the single separator character.
	 * @return the array of split items.
	 */
	public static String[] split(String str, char separatorChar) {
        if (str == null) {
            return null;
        }

        int length = str.length();

        if (length == 0) {
            return null;
        }

        List<String> list = new ArrayList<String>();
        int i = 0;
        int start = 0;
        boolean match = false;

        while (i < length) {
            if (str.charAt(i) == separatorChar) {
                if (match) {
                    list.add(str.substring(start, i));
                    match = false;
                }

                start = ++i;
                continue;
            }

            match = true;
            i++;
        }

        if (match) {
            list.add(str.substring(start, i));
        }

        return (String[]) list.toArray(new String[list.size()]);
    }
}