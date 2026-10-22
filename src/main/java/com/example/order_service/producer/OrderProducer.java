package com.example.order_service.producer;

import com.example.order_service.config.KafkaTopicConfig;
import com.example.order_service.model.Order;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, Order> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, Order> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<String, Order>> send(Order order) {
        try {
            return kafkaTemplate.send(KafkaTopicConfig.ORDERS_TOPIC, order)
                    .whenComplete((result, ex) -> {
                        if (ex == null) {
                            var meta = result.getRecordMetadata();
                            log.info("Sent '{}' -> partition {}, offset {}",
                                    order.orderId(), meta.partition(), meta.offset());
                        } else {
                            log.error("Async send failed for '{}'", order.orderId(), ex);
                        }
                    });
        } catch (RuntimeException ex) {
            log.error("Send failed immediately for '{}'", order.orderId(), ex);
            return CompletableFuture.failedFuture(ex);
        }
    }
}
