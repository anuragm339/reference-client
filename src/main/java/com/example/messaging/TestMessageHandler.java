package com.example.messaging;

import com.example.messaging.consumer.api.MessageConsumer;
import com.example.messaging.consumer.handler.MessageHandler;
import com.example.messaging.models.BatchMessage;
import com.example.messaging.models.Message;
import io.micronaut.context.ApplicationContext;
import io.rsocket.Payload;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.inject.Singleton;
import reactor.core.publisher.Mono;

@Singleton
public class TestMessageHandler implements MessageHandler {
    private static final Logger logger = LoggerFactory.getLogger(TestMessageHandler.class);
    @Inject
    private ApplicationContext applicationContext;

    @Override
    public Mono<Void> handleMessage(Message message) {
        try {
            logger.info("Processing TEST message: consumerId={}, offset={}, data={}",
                    message.getMsgOffset(),
                    new String(message.getData()));
            // Add your message processing logic here
            // For example:
            // process(message.getData());

            return Mono.empty(); // Return true to indicate successful processing

        } catch (Exception e) {
            logger.error("Error processing TEST message: {}", message.getMsgOffset(), e);
            return Mono.empty(); // Return false to indicate processing failure
        }
    }

    @Override
    public Mono<Payload> handleFluxMessage(Message message) {
        try {
            logger.info("Processing TEST message: consumerId={}, offset={}, data={}",
                    message.getMsgOffset(),
                    new String(message.getData()));

            // Add your message processing logic here
            // For example:
            // process(message.getData());

            return Mono.empty(); // Return true to indicate successful processing

        } catch (Exception e) {
            logger.error("Error processing TEST message: {}", message.getMsgOffset(), e);
            return Mono.empty(); // Return false to indicate processing failure
        }
    }

    @Override
    public Mono<Void> handleBatchMessage(BatchMessage batchMessage) {
        try {
            logger.info("Processing TEST batch message: batchId={}, count={}, type={}",
                    batchMessage.getBatchId(),
                    batchMessage.getBatchId(),
                    batchMessage.getType());

            // Add your batch message processing logic here
            // For example:
            // process(batchMessage.getData());

            return Mono.empty(); // Return true to indicate successful processing

        } catch (Exception e) {
            logger.error("Error processing TEST batch message: {}", batchMessage.getBatchId(), e);
            return Mono.empty(); // Return false to indicate processing failure
        }
    }
}
