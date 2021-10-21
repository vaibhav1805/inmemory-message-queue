package com.systems.mq.broker;

import com.systems.mq.consumer.Consumer;
import com.systems.mq.models.QItem;

import java.util.Optional;

public interface Broker<T> {
    int registerConsumer(Consumer<T> consumer);
    boolean unregisterConsumer(int consumerId, int topicId);
    Optional<QItem<T>> consume(int consumerId, int topicId);
    boolean produce(int topicId, T data);
}
