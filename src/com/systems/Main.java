package com.systems;

import com.systems.mq.broker.BasicBroker;
import com.systems.mq.broker.Broker;
import com.systems.mq.consumer.BasicConsumer;
import com.systems.mq.consumer.Consumer;
import com.systems.mq.models.QItem;
import com.systems.mq.producer.BasicProducer;
import com.systems.mq.producer.Producer;

import java.util.Optional;
import java.util.SplittableRandom;

/**
 * FUNCTIONAL REQUIREMENTS:
 * - Should support multi-tenancy (topics)
 * - Data stored should be resilient
 * - Data written is immutable
 * - Multiple consumers should be able to process data
 *
 * NON FUNCTION REQUIREMENTS
 * - Low latency
 * - TODO Concurrency
 */
public class Main {
    public static void main(String[] args) {
        Broker<String> node1 = new BasicBroker<>();
        Producer<String> p1 = new BasicProducer<>(node1);
        new SplittableRandom().ints(10, 0,3 )
                .forEach( i -> p1.produce(i, "test" + (int)(Math.random() * 3)));

        Consumer<String> c0 = new BasicConsumer<>(0,1, node1);
        Consumer<String> c1 = new BasicConsumer<>(1,2, node1);
        Consumer<String> c2 = new BasicConsumer<>(2,0, node1);


        System.out.println("Consumer 0 started");
        Optional<QItem<String>> s0 = c0.poll();
        while (s0.isPresent()){
            System.out.printf("c0 read from topic: %s, data: %s\n", c0.getTopic(),
                    s0.get().getData());
            s0 = c0.poll();
        }
//        ExecutorService executorService = Executors.newFixedThreadPool(5);
//        new SplittableRandom().ints(10, 0,3 )
//                .<Runnable>mapToObj(i -> () -> {
//                    p1.produce(i, "test" + (Math.random() * 3));
//                }).forEach(executorService::submit);
        System.out.println("Execution Successful");
    }
}
