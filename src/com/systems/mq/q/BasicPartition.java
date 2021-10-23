package com.systems.mq.q;

import com.systems.mq.models.QItem;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class BasicPartition<T> implements PartitionQueue<T>{
    /**
     * Data once written in partition is immutable
     * offsetMap ensures O(1) poll
     */
    private final BlockingDeque<QItem<T>> queue;
    private final Map<Integer, QItem<T>> offsetMap;
    private final ReentrantReadWriteLock lock;
    private int offset;
    private int capacity;

    public BasicPartition(int capacity){
        this.queue = new LinkedBlockingDeque<>();
        this.offsetMap = new ConcurrentHashMap<>();
        this.capacity = capacity;
        this.offset = -1;
        this.lock = new ReentrantReadWriteLock();
    }

    protected Optional<QItem<T>> poll() {
        if(!this.queue.isEmpty()){
            QItem<T> removedItem = this.queue.removeFirst();
            this.offsetMap.remove(removedItem.getOffset());
            return Optional.of(this.queue.removeFirst());
        }
        return Optional.empty();
    }

    @Override
    public Optional<QItem<T>> read(int offset) {
        lock.readLock().lock();
        try{
            if(offsetMap.containsKey(offset)){
                return Optional.of(offsetMap.get(offset));
            }
        }catch (Exception e){
            System.out.printf("Failed to read data from partition at offset:  %s\n", offset);
        }finally {
            lock.readLock().unlock();
        }
        return Optional.empty();
    }

    @Override
    public int add(T data) {
        lock.writeLock().lock();;
        try {
            if(this.size() >= capacity){
                this.poll();
            }
            offset++;
            QItem<T> item = new QItem<>(offset, data);
            this.queue.add(item);
            this.offsetMap.put(offset, item);
            return this.offset;
        }catch (Exception e){
            System.out.printf("Failed to add data to partition:  %s\n", data);
        }finally {
            lock.writeLock().unlock();
        }
        return -1;
    }

    @Override
    public int size() {
        return queue.size();
    }
}
