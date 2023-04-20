package com.taobao.drc.togo.data.schema;

/**
 * @author yangyang
 * @since 17/3/13
 */
public class SchemaBuilderException extends RuntimeException {
    public SchemaBuilderException(String s) {
        super(s);
    }

    public SchemaBuilderException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public SchemaBuilderException(Throwable throwable) {
        super(throwable);
    }
}
