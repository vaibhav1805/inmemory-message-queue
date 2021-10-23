package com.systems.mq.producer;

import com.systems.mq.broker.Broker;

public class BasicProducer<T> implements Producer<T>{
    private final Broker<T> broker;

    public BasicProducer(Broker<T> broker){
        this.broker = broker;
    }

    @Override
    public boolean produce(int topicId, T data) {
        System.out.printf("Pushing data to topicId: %s, data: %s\n", topicId, data);
        return broker.produce(topicId, data);
    }
}
