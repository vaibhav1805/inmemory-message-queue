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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
        syncProducer(p1);
        Consumer<String> c0 = new BasicConsumer<>(0,1, node1);
        Consumer<String> c1 = new BasicConsumer<>(1,2, node1);
        Consumer<String> c2 = new BasicConsumer<>(2,0, node1);

        ExecutorService producerService = Executors.newFixedThreadPool(5);
        CountDownLatch pLatch = new CountDownLatch(10);

        try{
            new SplittableRandom().ints(10, 0,3 ).parallel()
                    .<Runnable>mapToObj(i -> () -> {
                        p1.produce(i, "test" + (int)(Math.random() * 3));
                        pLatch.countDown();
                    }).forEach(producerService::submit);
            pLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producerService.shutdown();
        }


        ExecutorService consumerService = Executors.newFixedThreadPool(5);
        CountDownLatch cLatch = new CountDownLatch(10);
        try{
            new SplittableRandom().ints(10, 0,3 ).parallel()
                    .<Runnable>mapToObj(i -> () -> {
                        Optional<QItem<String>> s0 = c0.poll();
                        s0.ifPresent(stringQItem -> System.out.printf("c0 read from topic: %s, data: %s\n", c0.getTopic(),
                                stringQItem.getData()));
                        try {
                            Thread.sleep(2);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        cLatch.countDown();
                    }).forEach(consumerService::submit);
            cLatch.await();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            consumerService.shutdown();
        }

        System.out.println("Execution Successful");
    }

    private static <T> void syncProducer(Producer<T> p1){
        new SplittableRandom().ints(10, 0,3 )
                .forEach( i -> p1.produce(i, (T) ("test" + (int)(Math.random() * 3))));
    }

    private static <T> void syncConsumer(Consumer<T> c0){
        System.out.println("Consumer 0 started");
        Optional<QItem<T>> s0 = c0.poll();
        while (s0.isPresent()){
            System.out.printf("c0 read from topic: %s, data: %s\n", c0.getTopic(),
                    s0.get().getData());
            s0 = c0.poll();
        }
    }
}
