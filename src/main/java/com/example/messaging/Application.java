package com.example.messaging;

import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.core.ConsumerConfig;
import com.example.messaging.consumer.handler.DefaultMessageHandler;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);
    private static final List<MessageConsumer> consumers = new ArrayList<>();

    public static void main(String[] args) {
        ApplicationContext run = Micronaut.run(Application.class, args);

        int numberOfConsumers = getArgValue(args, "--consumers", 15);
        startConsumers(run, numberOfConsumers);

        // Keep the application running
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            stopConsumers();
        }));
    }

    private static void startConsumers(ApplicationContext context, int numberOfConsumers) {
        logger.info("Starting {} consumers", numberOfConsumers);
        CountDownLatch latch = new CountDownLatch(numberOfConsumers);
        TestMessageHandler messageHandlerBean = context.getBean(TestMessageHandler.class);

        Flux.range(0, numberOfConsumers)
                .flatMap(i -> {
                    String consumerId = "consumer-" + i;
                    String groupId = "group-" + (i); // Distribute across 5 groups
                    String messageType = "type-" + i;     // Unique type per consumer

                    ConsumerConfig config = ConsumerConfig.builder()
                            .serverHost("localhost")
                            .serverPort(7000)
                            .consumerId(consumerId)
                            .groupId(groupId)
                            .build();

                    // Create message handler for this consumer
                    DefaultMessageHandler messageHandler = new DefaultMessageHandler(consumerId);
                    messageHandler.registerHandler(messageType, message -> {
                        logger.info("Consumer {} processing message type: {}", consumerId, messageType);
                    });

                    // Create and configure consumer
                    ConsumerRSocketFactory factory = new ConsumerRSocketFactory(
                            messageHandler,
                            config,
                            new ObjectMapper()
                    );
                    MessageConsumer consumer = factory.createConsumer(config);

                    // Connect and store consumer
                    return consumer.connect()
                            .doOnSuccess(v -> {
                                consumers.add(consumer);
                                logger.info("Consumer {} started successfully", consumerId);
                                latch.countDown();
                            })
                            .doOnError(error -> {
                                logger.error("Failed to start consumer {}: {}", consumerId, error.getMessage());
                                latch.countDown();
                            });
                })
                .subscribe();

        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                logger.warn("Not all consumers started within timeout");
            }
            logger.info("Started {} consumers successfully", consumers.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while starting consumers", e);
        }
    }

    private static void stopConsumers() {
        logger.info("Stopping all consumers");
        CountDownLatch latch = new CountDownLatch(consumers.size());

        consumers.forEach(consumer -> {
            consumer.disconnect()
                    .doOnSuccess(v -> {
                        latch.countDown();
                        logger.info("Consumer disconnected successfully");
                    })
                    .doOnError(error -> {
                        logger.error("Error disconnecting consumer: {}", error.getMessage());
                        latch.countDown();
                    })
                    .subscribe();
        });

        try {
            if (!latch.await(30, TimeUnit.SECONDS)) {
                logger.warn("Not all consumers stopped within timeout");
            }
            consumers.clear();
            logger.info("All consumers stopped");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Interrupted while stopping consumers", e);
        }
    }

    private static int getArgValue(String[] args, String name, int defaultValue) {
        for (int i = 0; i < args.length - 1; i++) {
            if (args[i].equals(name)) {
                return Integer.parseInt(args[i + 1]);
            }
        }
        return defaultValue;
    }
}
