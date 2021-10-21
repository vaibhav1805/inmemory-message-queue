package com.systems.mq;

import com.systems.mq.broker.BasicBroker;
import com.systems.mq.broker.Broker;
import com.systems.mq.consumer.BasicConsumer;
import com.systems.mq.consumer.Consumer;

public class Driver {
    public static void main(String[] args) {
        Broker<String> node1 = new BasicBroker<>();
        Consumer<String> c0 = new BasicConsumer<>(0,1);
        Consumer<String> c1 = new BasicConsumer<>(1,2);
        Consumer<String> c2 = new BasicConsumer<>(2,0);

        node1.produce(0, "test");
        node1.produce(1, "test1");

        node1.registerConsumer(c0);
        node1.registerConsumer(c1);
        node1.registerConsumer(c2);
        System.out.println("Execution Successful");
    }
}
