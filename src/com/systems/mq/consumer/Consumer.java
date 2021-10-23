package com.systems.mq.consumer;

import com.systems.mq.models.QItem;

import java.util.Optional;

public interface Consumer<T> {
    Optional<QItem<T>> poll();
    Optional<QItem<T>> read(int offset);
    int getId();
    int getTopic();
    int getOffset();
}
