package com.aliyun.dts.subscribe.clients.common;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;

import java.nio.ByteBuffer;

public class GeometryUtil {

    private static final double SCALE = Math.pow(10.0D, 4.0);

    public static String fromWKBToWKTText(ByteBuffer data) throws ParseException {
        if (null == data) {
            return null;
        } else {
            WKBReader reader = new WKBReader();
            Geometry geometry = reader.read(data.array());
            return geometry.toText();
        }
    }
}
