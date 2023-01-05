package com.taobao.drc.client.transport.enums;

/**
 * Created by jianjundeng on 3/3/15.
 */
public enum DRCNetCompressState {

    MAGIC_NUMBER,

    SERVER_VERSION,

    GET_IDENTIFY,

    GET_HANDSHAKE,

    READ_COMPRESS_FLAG,

    READ_RAW_DATA_LENGTH,

    READ_COMPRESS_RAW_DATA_LENGTH,

    READ_COMPRESS_DATA,

    READ_RAW_DATA,
}
