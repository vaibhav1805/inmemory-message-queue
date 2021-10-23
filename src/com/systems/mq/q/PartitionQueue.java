package com.systems.mq.q;

import com.systems.mq.models.QItem;

import java.util.Optional;

public interface PartitionQueue<T> {
    Optional<QItem<T>> read(int offset);
    int add(T data);
    int size();
}
