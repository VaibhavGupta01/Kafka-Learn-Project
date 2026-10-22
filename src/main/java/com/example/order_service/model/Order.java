package com.example.order_service.model;

import java.math.BigDecimal;

public record Order(
        String orderId,
        String customerId,
        String product,
        int quantity,
        BigDecimal amount
) {}