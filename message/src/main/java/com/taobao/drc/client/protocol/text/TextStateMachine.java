package com.taobao.drc.client.protocol.text;

import com.taobao.drc.client.DRCClientException;
import com.taobao.drc.client.message.ByteString;
import com.taobao.drc.client.message.DataMessage.Record;
import com.taobao.drc.client.message.DataMessage.Record.Field;
import com.taobao.drc.client.message.DataMessage.Record.Type;
import com.taobao.drc.client.protocol.StateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.*;

import static com.taobao.drc.client.message.DataMessage.Record.Type.*;
import static com.taobao.drc.client.protocol.text.TextStateMachine.Status.*;

import static com.taobao.drc.client.utils.MessageConstant.*;

/**
 * Created by jianjundeng on 10/3/14.
 */
public class TextStateMachine implements StateMachine {
    private static final String ATTR_MESSAGE_ID = "message_id";
    private static final String ATTR_MESSAGE_TYPE = "message_type";

    private static final Logger log = LoggerFactory.getLogger(TextStateMachine.class);

    private Map<String, String> attrMap = new HashMap<String, String>();

    private Long prevMessageId = null;
    private Long messageId = null;
    private Integer messageType = null;

    private Record record= null;

    private List<String> primaryKeyList;

    private Field field=new Field();

    private Status status=READ_VERSION;

    private ByteArrayOutputStream attrHeaderLine = new ByteArrayOutputStream();

    private ByteArrayOutputStream key = new ByteArrayOutputStream();

    private ByteArrayOutputStream value = new ByteArrayOutputStream();

    private boolean parseKey=true;
    // Assume characters  before the first ':' is attribute name, characters until '\n' is attribute value
    private boolean firstReadSplitChar = false;

    private ByteArrayOutputStream name = new ByteArrayOutputStream();

    private ByteArrayOutputStream type = new ByteArrayOutputStream();

    private ByteArrayOutputStream fieldLength = new ByteArrayOutputStream();

    private static Map<String, Type> typeMap=new HashMap<String, Type>();

    static{
        typeMap.put("insert",INSERT);
        typeMap.put("update",UPDATE);
        typeMap.put("delete",DELETE);
        typeMap.put("replace",REPLACE);
        typeMap.put("heartbeat",HEARTBEAT);
        typeMap.put("consistency_test",CONSISTENCY_TEST);
        typeMap.put("begin",BEGIN);
        typeMap.put("commit",COMMIT);
        typeMap.put("ddl",DDL);
        typeMap.put("rollback",ROLLBACK);
        typeMap.put("dml",DML);
        typeMap.put("index_insert",INDEX_INSERT);
        typeMap.put("index_update",INDEX_UPDATE);
        typeMap.put("index_delete",INDEX_DELETE);
        typeMap.put("index_replace",INDEX_REPLACE);
    }

    private byte[] fieldValue;

    private int index;

    private long lastBytes = 0;

    private long len=0;


    public void parseByteBuf(ChannelHandlerContext ctx, ByteBuf byteBuf, int length) throws Exception {
        while (length>0) {
            byte b;
            switch (status) {
                case READ_VERSION:
                    b=byteBuf.readByte();
                    if (b == special_n) {
                        status=READ_MESSAGE;
                    }
                    length--;
                    break;
                case READ_MESSAGE:
                    b=byteBuf.readByte();
                    if (b != special_n) {
                        attrHeaderLine.write((char) b);
                    } else {
                        if (attrHeaderLine.size() == 0) {
                            status=READ_RECORD;

                            messageId = Long.parseLong(attrMap.get(ATTR_MESSAGE_ID));
                            messageType = Integer.parseInt(attrMap.get(ATTR_MESSAGE_TYPE));
                            if (prevMessageId != null && messageId != prevMessageId + 1) {
                                throw new DRCClientException("Invalid message id [" + messageId + "], previous message id is [" + prevMessageId + "]");
                            }
                            prevMessageId = messageId;

                            attrMap.clear();
                        } else {
                            // the end of the line
                            String line = attrHeaderLine.toString(DEFAULT_ENCODING);
                            String[] segs = StringUtils.split(line, ":", 2);
                            if (segs.length != 2) {
                                log.warn("Invalid message attribute: [" + line + "]");
                            } else {
                                attrMap.put(segs[0], segs[1]);
                            }
                        }
                        attrHeaderLine.reset();
                    }
                    length--;
                    break;
                case READ_RECORD:
                    //TODO: test if trace id with multi ':' can parsed correct, the following logic seems work well
                    b=byteBuf.readByte();
                    if (b == special_n) {
                        if(record==null){
                            status=READ_MESSAGE;
                        }else if (key.size() == 0) {
                            status=READ_FIELD_NAME;
                        } else {
                            String keyStr=key.toString(DEFAULT_ENCODING);
                            String valueStr=value.toString(DEFAULT_ENCODING);
                            record.addAttribute(keyStr, valueStr);
                            if(keyStr.equals("record_type")) {
                                record.setType(typeMap.get(valueStr));
                            }else if(keyStr.equals("primary")){
                                if(!StringUtils.isEmpty(valueStr)){
                                    primaryKeyList= Arrays.asList(valueStr.split(","));
                                }else{
                                    primaryKeyList=null;
                                }
                            }
                            key.reset();
                            value.reset();
                        }
                        parseKey=true;
                        firstReadSplitChar = false;
                    } else if(b==kv_split && firstReadSplitChar == false){
                        parseKey=false;
                        firstReadSplitChar = true;
                    } else if(parseKey){
                        key.write(b);
                    } else if(!parseKey){
                        value.write(b);
                        if(record==null){
                            record=new Record();
                            //
                            if(record.getFieldList()==null){
                                record.setFieldList(new ArrayList<Field>());
                            }
                        }
                    }
                    length--;
                    break;
                case READ_FIELD_NAME:
                    b=byteBuf.readByte();
                    if (b == special_n) {
                        if (name.size() == 0) {
                            status=READ_RECORD;
                            //notify
                            //handle encoding
                            overrideEncoding(record);
                            ctx.fireChannelRead(record);
                            record=null;
                            primaryKeyList=null;
                        } else {
                            status = READ_FIELD_TYPE;
                            String nameStr=name.toString(DEFAULT_ENCODING);
                            field.setFieldName(nameStr);
                            if(primaryKeyList!=null&&primaryKeyList.contains(nameStr)){
                                field.setPrimary(true);
                            }
                            field.setEncoding(record.getAttribute("record_encoding"));
                        }
                    } else {
                        name.write(b);
                    }
                    length--;
                    break;
                case READ_FIELD_TYPE:
                    b=byteBuf.readByte();
                    if (b == special_n) {
                        status = READ_FIELD_LENGTH;
                        int t=Integer.parseInt(type.toString(DEFAULT_ENCODING));
                        field.setType(t);
                    } else {
                        type.write(b);
                    }
                    length--;
                    break;
                case READ_FIELD_LENGTH:
                    b=byteBuf.readByte();
                    if (b == special_n) {
                        len = Long.parseLong(fieldLength.toString(DEFAULT_ENCODING));
                        lastBytes=len;
                        status = READ_FIELD_VALUE;
                        fieldValue=new byte[(int) lastBytes+1];
                    } else {
                        fieldLength.write(b);
                    }
                    length--;
                    break;
                case READ_FIELD_VALUE:
                    int available=length;
                    if (lastBytes<=0||lastBytes<=available) {
                        status = READ_FIELD_VALUE_END;
                        if (len >= 0) {
                            byteBuf.readBytes(fieldValue,index, (int) lastBytes);
                            field.setValue(new ByteString(fieldValue,(int)len));
                        }
                        record.getFieldList().add(field);
                        record.setRecordLength(record.getRecordLength() + len);
                        clearContext();
                        if(lastBytes>0) {
                            length -= lastBytes;
                        }
                    } else{
                        byteBuf.readBytes(fieldValue,index,available);
                        index+=available;
                        lastBytes-=available;
                        length=0;
                    }
                    break;
                case READ_FIELD_VALUE_END:
                    b=byteBuf.readByte();
                    if(b!=special_n){
                        throw new Exception("field parse error");
                    }
                    status=READ_FIELD_NAME;
                    length--;
                    break;
            }
        }
    }

    public enum Status{
        READ_VERSION,

        READ_MESSAGE,

        READ_RECORD,

        READ_FIELD_NAME,

        READ_FIELD_TYPE,

        READ_FIELD_LENGTH,

        READ_FIELD_VALUE,

        READ_FIELD_VALUE_END,
    }

    private void clearContext() {
        name.reset();
        type.reset();
        index=0;
        fieldValue=null;
        fieldLength.reset();
        field = new Field();
    }

    private void overrideEncoding(Record record) {
        String fieldsEncodings = record.getAttribute("fields_enc");
        if (fieldsEncodings != null && !fieldsEncodings.isEmpty()) {
            String[] encodings = fieldsEncodings.split(",",-1);
            List<Field> fields=record.getFieldList();
            if (encodings.length == fields.size()) {
                for (int i = 0; i < encodings.length; i++) {
                    String enc = encodings[i];
                    Field field = fields.get(i);
                    if (enc.isEmpty()) {
                        if (field.getType() == Field.Type.STRING) {
                            field.encoding = "binary";
                        } else if (field.getType() == Field.Type.JSON) {
                            field.encoding = "utf8mb4";
                        } else {
                            field.encoding = "";
                        }
                    } else {
                        if(field.getType()== Field.Type.BLOB){
                            field.type=15;
                        }
                        field.encoding = enc;
                    }
                }
            } else if (encodings.length * 2 == fields.size()) {
                for (int i = 0; i < encodings.length; i++) {
                    String enc = encodings[i];
                    Field field1 = fields.get(i * 2);
                    Field field2 = fields.get(i * 2 + 1);
                    if (enc.isEmpty()) {
                        if (field1.getType() == Field.Type.STRING) {
                            field1.encoding = "binary";
                            field2.encoding = "binary";
                        } else if (field1.getType() == Field.Type.JSON) {
                            field1.encoding = "utf8mb4";
                            field2.encoding = "utf8mb4";
                        } else {
                            field1.encoding = "";
                            field2.encoding = "";
                        }
                    } else {
                        if(field1.getType()== Field.Type.BLOB){
                            field1.type=15;
                            field2.type=15;
                        }
                        field1.encoding = enc;
                        field2.encoding = enc;
                    }
                }
            }
        }
    }
}
