package com.aliyun.dts.subscribe.clients.formats.util;

public class ObjectNameUtils {

    static final boolean checkAllNotNull(String... names) {
        if (null == names || names.length <= 0) {
            return true;
        }
        for (String name : names) {
            if (null != name) {
                return false;
            }
        }
        return true;
    }

    public static final String compressionObjectName(String... names) {

        if (checkAllNotNull(names)) {
            return null;
        }

        StringBuilder nameBuilder = new StringBuilder(128);
        for (String name : names) {
            if (nameBuilder.length() > 0) {
                nameBuilder.append(".");
            }
            nameBuilder.append(escapeName(name));
        }
        return nameBuilder.toString();
    }

    public static final String[] uncompressionObjectName(String compressionName) {
        if (null == compressionName || compressionName.isEmpty()) {
            return null;
        }

        String[] names = compressionName.split("\\.");

        int length = names.length;

        for (int i = 0; i < length; ++i) {
            names[i] = unescapeName(names[i]);
        }
        return names;
    }

    public static final String[] uncompressionObjectName(String compressionName, int limit) {
        if (null == compressionName || compressionName.isEmpty()) {
            return null;
        }

        String[] names = compressionName.split("\\.", limit);

        int length = names.length;

        for (int i = 0; i < length; ++i) {
            names[i] = unescapeName(names[i]);
        }
        return names;
    }

    static final String escapeName(String name) {
        if (null == name || (name.indexOf('.') < 0)) {
            return name;
        }

        StringBuilder builder = new StringBuilder();

        int length = name.length();

        for (int i = 0; i < length; ++i) {
            char c = name.charAt(i);
            if ('.' == c) {
                builder.append("\\u002E");
            } else {
                builder.append(c);
            }
        }

        return builder.toString();
    }

    static final String unescapeName(String name) {
        if (null == name || (name.indexOf("\\u002E") < 0)) {
            return name;
        }

        StringBuilder builder = new StringBuilder();

        int length = name.length();

        for (int i = 0; i < length; ++i) {
            char c = name.charAt(i);
            if ('\\' == c && (i < length - 5 && 'u' == name.charAt(i + 1)
                    && '0' == name.charAt(i + 2) && '0' == name.charAt(i + 3)
                    && '2' == name.charAt(i + 4) && 'E' == name.charAt(i + 5))) {
                builder.append(".");
                i += 5;
                continue;
            } else {
                builder.append(c);
            }
        }

        return builder.toString();
    }
}
