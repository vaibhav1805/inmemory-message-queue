package com.systems.mq.q;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

public class Topic<T> {
    private int id;
    private final Map<Integer, BasicPartition<T>> partitionMap;
    public static final int PARTITION_COUNT = 5;
    public static final int PARTITION_CAPACITY = 5;

    public Topic(int id){
        this.id = id;
        this.partitionMap = new HashMap<>();
        this.createPartitions(PARTITION_COUNT);
    }

    protected void createPartitions(int numberOfPartitions){
        IntStream.range(0, numberOfPartitions).forEach( i -> partitionMap.put(i, new BasicPartition<>(PARTITION_CAPACITY)));
    }

    public void push(T data){
        partitionMap.entrySet().stream().forEach(s -> {
            s.getValue().add(data);
        });
    }

    public int getId(){
        return id;
    }

    public  Map<Integer, BasicPartition<T>> getPartitionMap(){
        return this.partitionMap;
    }
}
