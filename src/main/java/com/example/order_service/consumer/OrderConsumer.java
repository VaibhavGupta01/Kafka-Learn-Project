package com.example.order_service.consumer;

import com.example.order_service.config.KafkaTopicConfig;
import com.example.order_service.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    @KafkaListener(topics = KafkaTopicConfig.ORDERS_TOPIC)
    public void onOrder(ConsumerRecord<String, Order> record) {
        Order order = record.value();
        log.info("Received order {} for customer {} ({} x {}) from partition {}, offset {}",
                order.orderId(), order.customerId(), order.quantity(), order.product(),
                record.partition(), record.offset());
    }
}
