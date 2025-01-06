package com.example.messaging;

import com.example.messaging.consumer.handler.DefaultMessageHandler;
import com.example.messaging.models.Message;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

@Factory
public class HandlerConfig {
    private static final Logger logger = LoggerFactory.getLogger(HandlerConfig.class);

    @Singleton
    DefaultMessageHandler messageHandler(TestMessageHandler testHandler) {
        DefaultMessageHandler handler = new DefaultMessageHandler("consumer-1");

        // Register TEST message handler
        handler.registerHandler("TEST", (Consumer<Message>) testHandler);

        logger.info("Initialized message handlers");
        return handler;
    }
}
