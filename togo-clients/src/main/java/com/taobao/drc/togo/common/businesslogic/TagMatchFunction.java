package com.taobao.drc.togo.common.businesslogic;

public enum  TagMatchFunction {
    FNMATCH("fnmatch", 0),
    REGEX("regex", 1);
    private final String functionName;
    private final int functionCode;

    TagMatchFunction(String functionName, int functionCode) {
        this.functionName = functionName;
        this.functionCode = functionCode;
    }

    public int getFunctionCode() {
        return functionCode;
    }

    public String getFunctionName() {
        return functionName;
    }

    public static TagMatchFunction codeToMatchFunction(int code) {
        return  code == 1 ? REGEX : FNMATCH;
    }
}
