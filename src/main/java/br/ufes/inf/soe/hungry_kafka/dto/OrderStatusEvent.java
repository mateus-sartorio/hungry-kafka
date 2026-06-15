package br.ufes.inf.soe.hungry_kafka.dto;

import java.time.Duration;
import java.time.Instant;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class OrderStatusEvent {

    private String eventId;
    private String orderId;
    private String userId;
    private String storeId;
    private String category;
    private OrderStatus status;
    private Duration expectedDelivery;
    private Instant occurredAt;

    public OrderStatusEvent() {
    }

}
