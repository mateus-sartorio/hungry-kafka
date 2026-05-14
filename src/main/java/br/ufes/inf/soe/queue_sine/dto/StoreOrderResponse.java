package br.ufes.inf.soe.queue_sine.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;
import java.util.List;

@Getter
@AllArgsConstructor
public class StoreOrderResponse {
    private Integer id;
    private ClientDto client;
    private List<OrderItemResponse> items;
    private Instant createdAt;
    private Instant expectedDelivery;
    private String status;
}
