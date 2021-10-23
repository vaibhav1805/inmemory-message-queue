package com.systems.mq.broker;

import com.systems.mq.consumer.Consumer;
import com.systems.mq.models.QItem;
import com.systems.mq.q.Topic;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class BasicBroker<T> implements Broker<T> {
    /**
     * Broker maintains following meta data
     * - Topics
     * - Connected Consumers along with what topics they are connected to.
     */
    private final Map<Integer, Topic<T>> topics;
    private final Map<Integer, Consumer<T>> consumers;
    private final Map<String, Integer> topicConsumerMap;

    public BasicBroker(){
        this.topics = new ConcurrentHashMap<>();
        this.consumers = new ConcurrentHashMap<>();
        this.topicConsumerMap = new ConcurrentHashMap<>();
    }

    //topicId_consumerId -> partitionId
    @Override
    public int registerConsumer(Consumer<T> consumer) {
        /**
         * Connects consumer to a partition of given topic
         * TODO: Move away from random partition allocation as one consumer in a group can connect to one partition only
         */
        if(!consumers.containsKey(consumer.getId())
                && topics.containsKey(consumer.getTopic())){
            int partiotionId = (int) (Math.random() * Topic.PARTITION_CAPACITY);
            consumers.put(consumer.getId(), consumer);
            topicConsumerMap.put(String.join("_",
                    String.valueOf(consumer.getTopic()), String.valueOf(consumer.getId())),
                    partiotionId);
            return partiotionId;
        }
        return -1;
    }

    @Override
    public boolean unregisterConsumer(int consumerId, int topicId) {
        //TODO
        return false;
    }

    @Override
    public Optional<QItem<T>> consume(int consumerId, int topicId, int offset) {
        if(topics.containsKey(topicId)){
            int partitionId = topicConsumerMap.get(String.join("_",
                    String.valueOf(topicId),
                    String.valueOf(consumerId)));
            return topics.get(topicId).getPartitionMap().get(partitionId).read(offset);
        }
        return Optional.empty();
    }

    @Override
    public boolean produce(int topicId, T data) {
        /**
         * Creates a new topic if not exists
         * Dispatches message to topic
         */
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
