package com.systems.mq.consumer;

public interface Consumer<T> {
    void connect();
    T poll();
    T read(int offset);
    int getId();
    int getTopic();
    int getOffset();
}
