package com.aliyun.dts.subscribe.clients.check;

public class CheckResult {
    public static final CheckResult SUCESS = new CheckResult(true, null);
    private boolean isOk;
    private String errMsg;

    public CheckResult(boolean isOk, String errMsg) {
        this.isOk = isOk;
        this.errMsg = errMsg;
    }

    public boolean isOk() {
        return isOk;
    }

    public void setOk(boolean ok) {
        isOk = ok;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    @Override
    public String toString() {
        return "CheckResult{" +
                "isOk=" + isOk +
                ", errMsg='" + errMsg + '\'' +
                '}';
    }
}
