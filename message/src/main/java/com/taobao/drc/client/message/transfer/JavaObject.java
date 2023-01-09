package com.taobao.drc.client.message.transfer;

public class JavaObject
{
	private int type;
	private Object objectValue;
	private boolean unsigned;
	private boolean outOfRange;
	
	public JavaObject() {
		objectValue = null;
		unsigned = outOfRange = false;
	}
	
	public int getType() {
		return type;
	}

	public Object getObjectValue() {
		return objectValue;
	}
	
	public boolean isUnsigned() {
		return unsigned;
	}
	
	public boolean isOutOfRange() {
		return outOfRange;
	}

	public void setType(int type) {
		this.type = type;
	}
	
	public void setObject(Object object) {
		objectValue = object;
	}
	
	public void setUnsigned(boolean unsigned) {
		this.unsigned = unsigned;
	}
	
	public void setOutOfRange(boolean outofrange) {
		this.outOfRange = outofrange;
	}
}
