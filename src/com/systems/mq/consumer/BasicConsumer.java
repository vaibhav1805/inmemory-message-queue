package com.systems.mq.consumer;

import com.systems.mq.broker.Broker;
import com.systems.mq.models.QItem;

import java.util.Optional;

public class BasicConsumer<T> implements Consumer<T> {
    /**
     * Consumer tracks the offset from which it has to read message
     * With read() consumer can access message at any offset
     */
    private final int consumerId;
    private int currentOffset;
    private final int topicId;
    private final Broker<T> broker;

    public BasicConsumer(int consumerId, int topicId, Broker<T> broker){
        this.consumerId = consumerId;
        this.currentOffset = -1;
        this.topicId = topicId;
        this.broker = broker;
        broker.registerConsumer(this);
    }

    @Override
    public Optional<QItem<T>> poll() {
        Optional<QItem<T>> item = this.read(currentOffset+1);
        if(item.isPresent())
            currentOffset++;
        return item;
    }

    @Override
    public Optional<QItem<T>> read(int offset) {
        return broker.consume(consumerId, topicId, offset);
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
