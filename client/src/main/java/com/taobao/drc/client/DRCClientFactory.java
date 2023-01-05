package com.taobao.drc.client;

import com.taobao.drc.client.impl.DRCClientImpl;

import java.io.IOException;
import java.io.Reader;
import java.util.Properties;

;

public class DRCClientFactory {
	
	public enum Type {
        MYSQL,
        OCEANBASE,
        HBASE,
        ORACLE,
        OCEANBASE1,
        NONE
	}

	public static DRCClient create(final Object arg) throws IOException {
			if (arg instanceof String) {
                return new DRCClientImpl((String) arg);
            }else if (arg instanceof Reader) {
                return new DRCClientImpl((Reader) arg);
            }else if (arg instanceof Properties) {
                return new DRCClientImpl((Properties) arg);
            }else {
                return null;
            }
	}
	
	public static DRCClient create(final Type type, final Object arg) throws IOException {
	    switch (type) {
            case MYSQL:
            case OCEANBASE:
            case OCEANBASE1: {
                return create(arg);
            }
            default: {
                return null;
            }
        }
	}
}
