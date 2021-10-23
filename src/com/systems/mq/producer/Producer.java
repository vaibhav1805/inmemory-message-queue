package com.systems.mq.producer;

public interface Producer<T> {
    boolean produce(int topicId, T data);
}
