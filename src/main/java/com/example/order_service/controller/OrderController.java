package com.example.order_service.controller;

import com.example.order_service.model.Order;
import com.example.order_service.producer.OrderProducer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping
    public CompletableFuture<ResponseEntity<String>> placeOrder(@RequestBody OrderRequest request) {
        var order = new Order(
                UUID.randomUUID().toString(),
                request.customerId(),
                request.product(),
                request.quantity(),
                request.amount());

        return orderProducer.send(order)
                .thenApply(result -> {
                    var meta = result.getRecordMetadata();
                    return ResponseEntity.accepted().body("Order " + order.orderId()
                            + " stored at partition " + meta.partition() + ", offset " + meta.offset());
                })
                .exceptionally(ex -> ResponseEntity.status(503)
                        .body("Could not publish order, please retry"));
    }

    public record OrderRequest(String customerId, String product, int quantity, BigDecimal amount) {}
}
