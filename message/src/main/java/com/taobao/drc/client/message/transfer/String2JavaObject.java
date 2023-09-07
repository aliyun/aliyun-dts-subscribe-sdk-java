package com.taobao.drc.client.message.transfer;

import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage.Record.Field;
import com.taobao.drc.client.message.DataMessage.Record.Field.Type;
import org.apache.commons.lang3.StringUtils;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.EnumMap;

public class String2JavaObject {

    private static final String MAX_INTEGER = Integer.toString(Integer.MAX_VALUE);
    private static final String MAX_LONG = Long.toString(Long.MAX_VALUE);
    private static Timestamp ZEROTIME = Timestamp.valueOf("1970-01-01 00:00:00");

    /* Used to transfer from drc field type to java object type. */
    private static final EnumMap<Type, Integer> jdbcTypes = new EnumMap<Type, Integer>(Type.class) {
        private static final long serialVersionUID = 1L;
                {
                    put(Type.INT8, Types.TINYINT);
                    put(Type.INT16, Types.SMALLINT);
                    put(Type.INT24, Types.INTEGER);
                    put(Type.INT32, Types.INTEGER);
                    put(Type.INT64, Types.BIGINT);
                    put(Type.DECIMAL, Types.DECIMAL);
                    put(Type.FLOAT, Types.FLOAT);
                    put(Type.DOUBLE, Types.DOUBLE);
                    put(Type.NULL, Types.NULL);
                    put(Type.TIMESTAMP, Types.TIMESTAMP);
                    put(Type.DATE, Types.DATE);
                    put(Type.TIME, Types.TIME);
                    put(Type.DATETIME, Types.TIMESTAMP);
                    put(Type.YEAR, Types.INTEGER);
                    put(Type.BIT, Types.BIT);
                    put(Type.ENUM, Types.CHAR);
                    put(Type.SET, Types.CHAR);
                    put(Type.BLOB, Types.BLOB);
                    put(Type.STRING, Types.VARCHAR);
                    put(Type.GEOMETRY, Types.BLOB);
                    put(Type.JSON, Types.VARCHAR);
                    put(Type.UNKOWN, Types.NULL);
                }
    };


    synchronized public static void setZeroTime(Timestamp ts) {
        ZEROTIME = ts;
    }

    synchronized public static final Timestamp getZeroTime() {
        return ZEROTIME;
    }

    /**
     * Transfer the drc field type to java object type
     *
     * @param type is the drc field type
     * @return the value of the java object type
     */
    public static int fieldType2Java(Type type) {
        return jdbcTypes.get(type);
    }

    /**
     * Transfer from string with field type to the java object
     *
     * @param field is the field value
     * @return the java object
     * @throws NumberFormatException
     * @throws UnsupportedEncodingException
     */
    public static JavaObject field2Java(final Field field)
            throws NumberFormatException, UnsupportedEncodingException {

        JavaObject ob = new JavaObject();
        ob.setType(jdbcTypes.get(field.getType()));

        /* Get field value rvalue */
        ByteString value = field.getValue();
        if (value == null)
            return ob;

        String rvalue = null;
        if (ob.getType() == Types.CHAR || ob.getType() == Types.VARCHAR) {
            if(!field.getEncoding().equals("binary")) {
                rvalue = value.toString(field.getEncoding());
            } else {
                ob.setType(Types.BLOB);
            }
        } else if (ob.getType() != Types.BLOB) {
            rvalue = value.toString();
        }

        /* Get java object */

        switch (ob.getType()) {
            case Types.BIT:
                bitstr2Object(rvalue, ob);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
                ob.setObject(Integer.valueOf(rvalue));
                break;
            case Types.INTEGER:
                intstr2Object(rvalue, ob);
                break;
            case Types.BIGINT:
                bigintstr2object(rvalue, ob);
                break;
            case Types.DECIMAL:
                ob.setObject(new BigDecimal(rvalue));
                break;
            case Types.FLOAT:
                ob.setObject(Float.valueOf(rvalue));
                break;
            case Types.DOUBLE:
                ob.setObject(Double.valueOf(rvalue));
                break;
            case Types.TIMESTAMP:
                timestamp2object(rvalue, ob);
                break;
            case Types.DATE:
                date2object(rvalue,ob);
                break;
            case Types.TIME:
                time2object(rvalue, ob);
                break;
            case Types.BLOB:
                ob.setObject(value.getBytes());
                break;
            case Types.CHAR:
            case Types.VARCHAR:
                ob.setObject(rvalue);
                break;
            case Types.NULL:
            default:
                ob.setObject(null);
        }

        return ob;
    }


    private static Object bitstr2Object(final String rvalue, final JavaObject ob) {
        int ret = rvalue.compareTo("1");
        if (ret > 0) {
            BigInteger bi = new BigInteger(rvalue);
            byte[] bytes = bi.toByteArray();
            int realLength = bytes.length;
            int i = 0;
            for (; i < bytes.length; i++) {
                if (bytes[i] != 0) {
                    break;
                } else {
                    realLength--;
                }
            }
            byte[] realbyte = new byte[realLength];
            for (int j = i; j < bytes.length; j++) {
                realbyte[j - i] = bytes[j];
            }
            ob.setObject(realbyte);
        } else {
            ob.setObject(((ret == 0) ? true : false));
        }
        return ob;
    }

    private static Object intstr2Object(final String rvalue, final JavaObject ob) {
        int rlength = rvalue.length();
        int maxLength = MAX_INTEGER.length();
        if (rvalue.contains("-") || rlength < maxLength ||
                (rlength == maxLength && rvalue.compareTo(MAX_INTEGER) < 0)) {
            ob.setObject(Integer.valueOf(rvalue));
        } else {
            ob.setUnsigned(true);
            ob.setObject(Long.valueOf(rvalue));
        }
        return ob;
    }

    private static Object bigintstr2object(final String rvalue, final JavaObject ob) {
        int rlength = rvalue.length();
        int maxLength = MAX_LONG.length();
        if (rvalue.contains("-") || rlength < maxLength ||
                (rlength == maxLength && rvalue.compareTo(MAX_LONG) < 0)) {
            ob.setObject(Long.valueOf(rvalue));
        } else {
            ob.setUnsigned(true);
            ob.setObject(new BigInteger(rvalue));
        }
        return ob;
    }

    private static Object timestamp2object(final String rvalue, final JavaObject ob) {
        Object objectValue;
        try {
        	 /* assume the format is ssssssssss[.f...] */
            long secondsRepresentedByMilli;
            secondsRepresentedByMilli = Long.parseLong(StringUtils.substringBefore(rvalue, ".")) * 1000;
            String fractionalRepresentedByNano = StringUtils.substringAfter(rvalue, ".");
            if (fractionalRepresentedByNano.contains(".") || fractionalRepresentedByNano.length() > 9) {
                objectValue=rvalue;
                ob.setOutOfRange(true);
            } else {
                fractionalRepresentedByNano = String.format("%-9s", fractionalRepresentedByNano).replace(" ", "0");
                objectValue = new Timestamp(secondsRepresentedByMilli);
                try {
                    ((Timestamp)objectValue).setNanos(Integer.parseInt(fractionalRepresentedByNano));
                } catch (NumberFormatException ne) {
                    objectValue=rvalue;
                    ob.setOutOfRange(true);
                }
            }
        } catch (IllegalArgumentException e) {
        	/* assume the format is yyyy-mm-dd hh:mm:ss[.f...] */
            try {
                objectValue = Timestamp.valueOf(rvalue);
            } catch(Exception err) {
                objectValue=rvalue;
                ob.setOutOfRange(true);
            }
        }
        ob.setObject(objectValue);
        return ob;
    }


    private static Object date2object(final String rvalue,final JavaObject ob){
        Object objectValue;
        try {
            objectValue = java.sql.Date.valueOf(rvalue.replaceAll("[^0-9]","-"));
        }catch (Exception e){
            objectValue=rvalue;
            ob.setOutOfRange(true);
        }
        ob.setObject(objectValue);
        return ob;
    }

    private static Object time2object(final String rvalue, final JavaObject ob) {
        Object objectValue = java.sql.Time.valueOf(rvalue);
        if (!((java.sql.Time) objectValue).toString().equals(rvalue)) {
            ob.setOutOfRange(true);
        }
        ob.setObject(objectValue);
        return ob;
    }
}

