package com.systems.mq.q;

import com.systems.mq.models.QItem;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;

public class BasicPartition<T> implements PartitionQueue<T>{
    private final LinkedList<QItem<T>> queue;
    private final Map<Integer, QItem<T>> offsetMap;
    private int offset;
    private int capacity;

    public BasicPartition(int capacity){
        this.queue = new LinkedList<>();
        this.offsetMap = new HashMap<>();
        this.capacity = capacity;
        this.offset = -1;
    }

    @Override
    public Optional<QItem<T>> poll() {
        if(!this.queue.isEmpty()){
            QItem<T> removedItem = this.queue.removeFirst();
            this.offsetMap.remove(removedItem.getOffset());
            return Optional.of(this.queue.removeFirst());
        }
        return Optional.empty();
    }

    @Override
    public Optional<QItem<T>> read(int offset) {
        if(offsetMap.containsKey(offset)){
            return Optional.of(offsetMap.get(offset));
        }
        return Optional.empty();
    }

    @Override
    public int add(T data) {
        if(this.size() >= capacity){
            this.poll();
        }
        offset++;
        this.queue.add(new QItem<>(offset, data));
        return this.offset;
    }

    @Override
    public int size() {
        return queue.size();
    }
}