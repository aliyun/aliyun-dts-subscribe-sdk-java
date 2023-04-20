package com.taobao.drc.togo.common.businesslogic;



import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by longxuan on 18/1/23.
 */
public class HashTypeFilterTag {
    private static final int INVALID_MAX_HASH_MOD_VALUE = 0;
    public static final HashTypeFilterTag DEFAULT_HASH_TYPE_TAG = new HashTypeFilterTag(INVALID_MAX_HASH_MOD_VALUE);
    private int maxHashModValue;
    private List<Integer> subHashList;
    public HashTypeFilterTag(int maxHashModValue) {
        this.maxHashModValue = maxHashModValue;
        subHashList = new ArrayList<>();
    }

    public void setMaxHashValue(int maxHashModValue) {
        this.maxHashModValue = maxHashModValue;
    }

    public int getMaxHashModValue() {
        return maxHashModValue;
    }

    public List<Integer> getSubHashList() {
        return subHashList;

    }
    public void addHashElement(Integer hashElement) {
        subHashList.add(hashElement);
    }

    public void addHashElements(Collection<Integer> hashElements) {
        subHashList.addAll(hashElements);
    }

    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("MaxHashModValue:").append(maxHashModValue).append("\n")
                .append("HashList:").append("[");
        if (null != subHashList) {
            for (Integer v : subHashList) {
                stringBuilder.append(v).append(",");
            }
        }
        stringBuilder.append("]\n");
        return stringBuilder.toString();
    }

    public boolean isHashValid() {
        return maxHashModValue != INVALID_MAX_HASH_MOD_VALUE;
    }

}
