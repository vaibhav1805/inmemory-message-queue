package com.systems.mq.broker;

import com.systems.mq.consumer.Consumer;
import com.systems.mq.models.QItem;
import com.systems.mq.q.Topic;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BasicBroker<T> implements Broker<T> {
    private final Map<Integer, Topic<T>> topics;
    private final Map<Integer, Consumer<T>> consumers;
    private final Map<String, Integer> topicConsumerMap;

    public BasicBroker(){
        this.topics = new HashMap<>();
        this.consumers = new HashMap<>();
        this.topicConsumerMap = new HashMap<>();
    }

    //topicId_consumerId -> partitionId
    @Override
    public int registerConsumer(Consumer<T> consumer) {
        if(!consumers.containsKey(consumer.getId())
                && topics.containsKey(consumer.getTopic())){
            int partiotionId = (int) (Math.random() * Topic.PARTITION_CAPACITY);
            consumers.put(consumer.getId(), consumer);
            topicConsumerMap.put(String.join("-",
                    String.valueOf(consumer.getTopic()), String.valueOf(consumer.getId())),
                    partiotionId);
            return partiotionId;
        }
        return -1;
    }

    @Override
    public boolean unregisterConsumer(int consumerId, int topicId) {
        return false;
    }

    @Override
    public Optional<QItem<T>> consume(int consumerId, int topicId) {
        if(topics.containsKey(topicId)){
            int partitionId = topicConsumerMap.get(String.join("_",
                    String.valueOf(topicId),
                    String.valueOf(consumerId)));
            //TODO find offset
            return topics.get(topicId).getPartitionMap().get(partitionId).read(1);
        }
        return Optional.empty();
    }

    @Override
    public boolean produce(int topicId, T data) {
        try{
            Topic<T> topic = topics.getOrDefault(topicId, new Topic<>(topicId));
            if(!topics.containsKey(topicId))
                topics.put(topicId, topic);
            topic.push(data);
            return true;
        }catch (Exception e){
            System.out.printf("Failed to produce data: %s", e.getMessage());
            return false;
        }
    }
}
