package com.taobao.drc.togo.util.acl;

import java.util.HashMap;

/**
 * Created by longxuan on 17/12/1.
 */
public enum ACLType {
    READ_ONLY(1, "read_only"),
    WRITER_ONLY(2, "write_only"),
    SUPER(3, "super"),
    INVALID(4, "invalid");
    private int code;
    private String describe;
    ACLType(int code, String describe) {
        this.code = code;
        this.describe = describe;
    }
    public int getCode() {
        return code;
    }
    public String getDescribe() {
        return describe;
    }

    private static HashMap<Integer, ACLType> codeToACLType;
    private static HashMap<String, ACLType> nameToACLType;
    static {
        codeToACLType = new HashMap<>();
        nameToACLType = new HashMap<>();
        for (ACLType aclType : ACLType.values()) {
            codeToACLType.put(aclType.code, aclType);
            nameToACLType.put(aclType.describe, aclType);
        }
    }

    public static ACLType fromCode(int code) {
        return codeToACLType.getOrDefault(code, INVALID);
    }

    public static ACLType fromDescribe(String describe) {
        return nameToACLType.getOrDefault(describe, INVALID);
    }

}
