package com.systems.mq.models;

public class QItem<T> {
    private int offset;
    private long timestamp;
    private T data;

    public QItem(int offset, T data){
        this.data = data;
        this.offset = offset;
        this.timestamp = System.currentTimeMillis();
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
