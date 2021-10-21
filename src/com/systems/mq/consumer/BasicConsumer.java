package com.systems.mq.consumer;

public class BasicConsumer<T> implements Consumer<T> {
    private final int consumerId;
    private final int currentOffset;
    private final int topicId;

    public BasicConsumer(int consumerId, int topicId){
        this.consumerId = consumerId;
        this.currentOffset = 0;
        this.topicId = topicId;
    }
    @Override
    public void connect() {

    }

    @Override
    public T poll() {
        return null;
    }

    @Override
    public T read(int offset) {
        return null;
    }

    @Override
    public int getId() {
        return this.consumerId;
    }

    @Override
    public int getTopic() {
        return this.topicId;
    }

    @Override
    public int getOffset() {
        return this.currentOffset;
    }
}
