package com.example.messaging;

import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.core.ConsumerConfig;
import com.example.messaging.consumer.handler.DefaultMessageHandler;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.consumer.rsocket.impl.ConsumerRSocketFactory;
import com.example.messaging.models.Message;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import io.micronaut.runtime.Micronaut;
import reactor.core.publisher.Mono;

import java.util.function.Consumer;

public class Application {
    public static void main(String[] args) {
        ApplicationContext run = Micronaut.run(Application.class, args);

        testMono(run);

    }
    private static void testMono(ApplicationContext run) {
        ConsumerConfig config = ConsumerConfig.builder()
                .serverHost("localhost")
                .serverPort(7000)
                .consumerId("consumer-1")
                .groupId("group-1")
                .build();

        TestMessageHandler bean = run.getBean(TestMessageHandler.class);
        DefaultMessageHandler messageHandler = new DefaultMessageHandler(config.getConsumerId());
        messageHandler.registerHandler("TEST", message -> {
            bean.handleMessage(message).subscribe();

        });
        ConsumerRSocketFactory ConsumerRSocketFactory = new ConsumerRSocketFactory(messageHandler, config, new ObjectMapper());
        MessageConsumer consumer = ConsumerRSocketFactory.createConsumer(config);

        consumer.connect().subscribe();

    }
}
