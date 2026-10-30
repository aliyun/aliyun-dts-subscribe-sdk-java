package com.taobao.drc.client.impl;

/** Internal callback scope: stopping from a listener must never join listener threads. */
public final class ListenerContext {
    private static final ThreadLocal<Integer> DEPTH = new ThreadLocal<Integer>();
    private ListenerContext() { }
    public static boolean isCallbackThread() { return DEPTH.get() != null; }
    public static void enter() { Integer depth = DEPTH.get(); DEPTH.set(depth == null ? 1 : depth + 1); }
    public static void exit() {
        int depth = DEPTH.get();
        if (depth == 1) { DEPTH.remove(); } else { DEPTH.set(depth - 1); }
    }
}
